from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog
from a2a.types.a2a_pb2 import Part
from google.protobuf.json_format import MessageToDict
from litellm.types.llms.openai import (
    ChatCompletionToolMessage,
    OpenAIChatCompletionAssistantMessage,
    OpenAIChatCompletionUserMessage,
)

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
        """Reconstruct messages from decision / function-call artifacts.

        Task ``history`` is deliberately ignored: it is a second, unordered
        channel relative to artifacts, and every message written to
        ``runtime.messages`` is also emitted as an artifact in the same order.
        Replaying artifacts alone therefore reproduces the live message list
        exactly (and cannot duplicate user messages).
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
            for artifact in task.artifacts or []:
                for part in artifact.parts:
                    cw_field = part.metadata.fields.get("cw_type")
                    if cw_field is None:
                        continue
                    cw_type = cw_field.string_value
                    if cw_type == CwType.COMPACTION:
                        if part.WhichOneof("content") != "text":
                            continue
                        count_field = part.metadata.fields.get("compacted_count")
                        count = int(count_field.number_value) if count_field else 0
                        latest_compaction = CompactionState(
                            compacted_count=count, summary=part.text
                        )
                    elif cw_type in (CwType.DECISION, CwType.HISTORY):
                        if part.WhichOneof("content") != "text":
                            continue
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
                    elif cw_type == CwType.FUNCTION_CALL:
                        if part.WhichOneof("content") != "data":
                            continue
                        raw.extend(_tool_exchange_from_data(part))

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
        while split > 0 and split < len(messages) and messages[split].get("role") == "tool":
            split -= 1
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
        for call in msg.get("tool_calls") or []:
            function = call.get("function") or {}
            name = function.get("name") or ""
            arguments = function.get("arguments") or ""
            if name or arguments:
                lines.append(f"[tool_call] {name} {arguments}")
    return "\n".join(lines)


def _tool_exchange_from_data(part: Part) -> list[ChatMessage]:
    """Rebuild the assistant tool call + tool result pair from an artifact."""
    payload = MessageToDict(part.data, preserving_proto_field_name=True)
    function_name = str(payload.get("function_name", ""))
    if not function_name:
        return []
    call_id = payload.get("call_id")
    if not isinstance(call_id, str) or not call_id:
        call_id = uuid.uuid4().hex
    args_json = payload.get("args_json")
    if not isinstance(args_json, str) or not args_json:
        args_json = json.dumps(
            payload.get("function_args", {}), ensure_ascii=False, separators=(",", ":")
        )
    result_json = payload.get("result_json")
    if not isinstance(result_json, str) or not result_json:
        result_json = json.dumps(
            payload.get("function_result", {}), ensure_ascii=False, separators=(",", ":")
        )
    return [
        OpenAIChatCompletionAssistantMessage(
            role="assistant",
            content=None,
            tool_calls=[
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": function_name, "arguments": args_json},
                }
            ],
        ),
        ChatCompletionToolMessage(role="tool", tool_call_id=call_id, content=result_json),
    ]
