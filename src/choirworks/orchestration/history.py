from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog
from a2a.types.a2a_pb2 import Role
from litellm.types.llms.openai import (
    OpenAIChatCompletionAssistantMessage,
    OpenAIChatCompletionUserMessage,
)

from choirworks.a2a.room import message_text
from choirworks.a2a.tasks import list_all_tasks
from choirworks.core.events import CwType
from choirworks.core.llm import ChatMessage, LiteLLMClient

if TYPE_CHECKING:
    from a2a.server.tasks.task_store import TaskStore

logger = structlog.get_logger(__name__)

SUMMARIZE_PROMPT = """You are summarizing a group chat history for an AI assistant.
Condense the following messages into a brief summary preserving:
- Key decisions and their rationale
- Completed work and outputs
- Unresolved questions and pending tasks
- Agent assignments and roles

Be concise. Output only the summary."""


@dataclass(frozen=True, slots=True)
class CompactionState:
    """In-memory tracking of the latest compaction for a conversation."""

    compacted_count: int
    summary: str


@dataclass(frozen=True, slots=True)
class CompactionResult:
    """Result of a compaction for the caller to persist."""

    compacted_count: int
    summary: str


class HistoryBuilder:
    """Builds and compacts conversation history for LLM context.

    ``rebuild`` reconstructs ``list[ChatMessage]`` from the A2A task store
    (used on restart). ``compact`` folds older messages into an LLM-summarized
    assistant message when token count exceeds ``compaction_threshold`` of
    the context window. The compaction state (how many raw messages were
    covered) is tracked in memory and recovered from task store artifacts
    on restart.
    """

    def __init__(
        self,
        llm: LiteLLMClient,
        task_store: TaskStore,
        *,
        compaction_threshold: float = 0.8,
        retention: int = 10,
    ):
        self._llm = llm
        self._task_store = task_store
        self._compaction_threshold = compaction_threshold
        self._retention = retention
        self._compaction_state: dict[str, CompactionState] = {}

    async def rebuild(self, context_id: str, *, exclude_task_id: str = "") -> list[ChatMessage]:
        """Reconstruct messages from the A2A task store (restart recovery).

        Reads user/agent messages from ``task.history`` and decision/compaction
        artifacts from ``task.artifacts``, then applies the latest compaction.
        """
        if not context_id:
            return []
        try:
            tasks = await list_all_tasks(self._task_store, context_id=context_id, reverse=True)
        except Exception:  # noqa: BLE001 - best effort
            return []

        raw: list[ChatMessage] = []
        latest_compaction: CompactionState | None = None

        for task in tasks:
            if exclude_task_id and task.id == exclude_task_id:
                continue
            for msg in task.history or []:
                text = message_text(msg)
                if not text:
                    continue
                if msg.role == Role.ROLE_USER:
                    raw.append(OpenAIChatCompletionUserMessage(role="user", content=text))
                else:
                    raw.append(OpenAIChatCompletionAssistantMessage(role="assistant", content=text))
            for artifact in task.artifacts or []:
                for part in artifact.parts:
                    if part.WhichOneof("content") != "text":
                        continue
                    cw_type = part.metadata.fields.get("cw_type")
                    if cw_type is None:
                        continue
                    if cw_type.string_value == CwType.COMPACTION:
                        count_field = part.metadata.fields.get("compacted_count")
                        count = int(count_field.number_value) if count_field else 0
                        latest_compaction = CompactionState(
                            compacted_count=count, summary=part.text
                        )
                    elif cw_type.string_value == CwType.DECISION:
                        role_field = part.metadata.fields.get("role")
                        role = role_field.string_value if role_field else "assistant"
                        if role == "user":
                            raw.append(
                                OpenAIChatCompletionUserMessage(role="user", content=part.text)
                            )
                        else:
                            raw.append(
                                OpenAIChatCompletionAssistantMessage(
                                    role="assistant", content=part.text
                                )
                            )

        if latest_compaction is not None:
            self._compaction_state[context_id] = latest_compaction
            summary_msg: ChatMessage = OpenAIChatCompletionAssistantMessage(
                role="assistant", content=latest_compaction.summary
            )
            return [summary_msg, *raw[latest_compaction.compacted_count :]]

        return raw

    async def compact(
        self, messages: list[ChatMessage], context_id: str
    ) -> tuple[list[ChatMessage], CompactionResult | None]:
        """Fold older messages into a summary when tokens exceed threshold.

        Returns ``(compacted_messages, result)``. ``result`` is ``None`` when
        no compaction was needed; otherwise the caller should persist the
        compaction artifact so ``rebuild`` can recover it on restart.
        """
        total_text = _messages_text(messages)
        token_count = self._llm.count_tokens(total_text)
        threshold = int(self._llm.get_context_window() * self._compaction_threshold)
        if token_count <= threshold:
            return (messages, None)

        split = max(len(messages) - self._retention, 0)
        if split == 0:
            return (messages, None)

        old = messages[:split]
        recent = messages[split:]

        state = self._compaction_state.get(context_id)
        if state is not None:
            new_msgs = old[1:]
            new_text = _messages_text(new_msgs)
            if new_text:
                summary = await self._llm.text(
                    system=SUMMARIZE_PROMPT,
                    user=f"Previous summary:\n{state.summary}\n\nNew messages:\n{new_text}",
                )
            else:
                summary = state.summary
            new_compacted_count = state.compacted_count + len(new_msgs)
        else:
            summary = await self._llm.text(system=SUMMARIZE_PROMPT, user=_messages_text(old))
            new_compacted_count = split

        self._compaction_state[context_id] = CompactionState(
            compacted_count=new_compacted_count, summary=summary
        )

        summary_msg: ChatMessage = OpenAIChatCompletionAssistantMessage(
            role="assistant", content=summary
        )
        result = CompactionResult(compacted_count=new_compacted_count, summary=summary)
        return ([summary_msg, *recent], result)


def _messages_text(messages: list[ChatMessage]) -> str:
    lines: list[str] = []
    for msg in messages:
        content = msg.get("content", "")
        role = msg.get("role", "")
        if content:
            lines.append(f"[{role}] {content}")
    return "\n".join(lines)
