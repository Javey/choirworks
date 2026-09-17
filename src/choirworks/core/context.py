from __future__ import annotations

# Context assembly is kept separate from orchestration, structured after
# google-adk's flows/llm_flows (Apache-2.0, https://github.com/google/adk-python),
# which splits prompt construction into dedicated modules.
from collections.abc import Iterable, Sequence

from a2a.server.context import ServerCallContext
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import ListTasksRequest, Role

from choirworks.a2a.room import message_text, room_options
from choirworks.a2a.state import NodeState
from choirworks.core.fencing import (
    QUOTED_CONTENT_PREAMBLE,
    cap_description,
    quote_untrusted,
)
from choirworks.core.llm import LiteLLMClient
from choirworks.models.domain import AgentRecord

MAX_PEER_CONTEXT = 2000

SUMMARIZE_PROMPT = """You are summarizing a group chat history for an AI orchestrator.
Condense the following messages into a brief summary preserving:
- Key decisions and their rationale
- Completed work and outputs
- Unresolved questions and pending tasks
- Agent assignments and roles

Be concise. Output only the summary."""


def build_planner_capabilities(agents: Sequence[AgentRecord]) -> str:
    lines = []
    for agent in agents:
        skills = agent.card.get("skills", [])
        skill_text = "; ".join(
            f"{skill.get('id')} ({cap_description(str(skill.get('description', '')))})"
            for skill in skills
        )
        description = cap_description(str(agent.card.get("description", "")))
        lines.append(
            f"- {agent.name}: {description} skills=[{skill_text}]"
        )
    return "\n".join(lines)


def build_planner_user_message(
    request: str,
    capabilities: str,
    *,
    reason: str | None = None,
    context: str | None = None,
) -> str:
    user = (
        f"User request:\n{request}\n\n"
        f"{QUOTED_CONTENT_PREAMBLE}\n"
        f"{quote_untrusted('Available agents:\n' + capabilities)}"
    )
    if reason:
        user += f"\n\n{quote_untrusted('Reason for replanning:\n' + reason)}"
    if context:
        user += f"\n\n{quote_untrusted('Completed work so far:\n' + context)}"
    return user


def build_assistance_decision_user(
    requester_name: str,
    blocked_text: str,
    candidates: Sequence[AgentRecord],
) -> str:
    capabilities = "\n".join(
        f"- {agent.name}: {cap_description(str(agent.card.get('description', '')))}"
        for agent in candidates
    )
    return (
        f"Requester: {requester_name}\n"
        f"Question / blocked work:\n{blocked_text}\n\n"
        f"{QUOTED_CONTENT_PREAMBLE}\n"
        f"{quote_untrusted('Available agents:\n' + capabilities)}"
    )


def build_replan_reason(nodes: Iterable[NodeState]) -> str:
    errors = ", ".join(
        node.error or node.id for node in nodes if node.status == "failed"
    )
    return f"nodes failed: {errors}"


def build_replan_context(nodes: Iterable[NodeState]) -> str:
    return "\n".join(
        f"- @{node.agent_name}: {(node.output or '')[:400]}"
        for node in nodes
        if node.status == "completed"
    )


def build_followup_input(
    anchor_agent_name: str, anchor_output: str | None, text: str
) -> str:
    if not anchor_output:
        return text
    return (
        f"引用 @{anchor_agent_name} 此前产出：\n"
        f"{quote_untrusted(anchor_output[:MAX_PEER_CONTEXT])}\n\n"
        f"新要求：{text}"
    )


def build_assist_input(requester_name: str, output: str | None) -> str:
    return (
        f"@{requester_name} 在协作中请求你的协助。\n"
        f"参考上下文：\n{quote_untrusted((output or '')[:MAX_PEER_CONTEXT])}\n\n"
        f"请提供你的专业协助。"
    )


def build_peer_fallback_input(blocked_text: str) -> str:
    return f"请协助回答以下问题：\n{blocked_text}"


class ContextBriefBuilder:
    """Builds the cross-task conversation brief with summary compaction."""

    def __init__(
        self,
        llm: LiteLLMClient,
        *,
        compaction_threshold: float = 0.8,
        compaction_retention: int = 10,
    ):
        self._llm = llm
        self._compaction_threshold = compaction_threshold
        self._compaction_retention = compaction_retention
        self._task_store: TaskStore | None = None
        self._cache: dict[str, tuple[str, int]] = {}

    def set_task_store(self, task_store: TaskStore) -> None:
        self._task_store = task_store

    async def build(self, context_id: str, exclude_task_id: str) -> str:
        if not context_id or self._task_store is None:
            return ""
        try:
            params = ListTasksRequest(context_id=context_id, page_size=50)
            page = await self._task_store.list(params, ServerCallContext())
        except Exception:  # noqa: BLE001 - context is best effort
            return ""

        timeline: list[str] = []
        for task in page.tasks:
            if task.id == exclude_task_id:
                continue
            for msg in task.history or []:
                rm = room_options(msg)
                sender = (
                    rm.get("sender")
                    or ("user" if msg.role == Role.ROLE_USER else "agent")
                )
                text = message_text(msg)
                if text:
                    timeline.append(f"[{sender}] {text}")

        if not timeline:
            return ""

        full_text = "\n".join(timeline)
        threshold = int(
            self._llm.get_context_window() * self._compaction_threshold
        )
        if self._llm.count_tokens(full_text) <= threshold:
            return full_text

        retention = min(self._compaction_retention, len(timeline))
        split = len(timeline) - retention
        recent = timeline[split:]
        old = timeline[:split]

        cached = self._cache.get(context_id)
        if cached and cached[1] == split:
            summary = cached[0]
        elif cached and cached[1] < split:
            new_msgs = "\n".join(timeline[cached[1]:split])
            summary = await self._llm.text(
                system=SUMMARIZE_PROMPT,
                user=f"Previous summary:\n{cached[0]}\n\nNew messages:\n{new_msgs}",
            )
            self._cache[context_id] = (summary, split)
        else:
            summary = await self._llm.text(
                system=SUMMARIZE_PROMPT,
                user="\n".join(old),
            )
            self._cache[context_id] = (summary, split)

        return f"## 群聊历史摘要\n{summary}\n\n## 最近消息\n" + "\n".join(recent)
