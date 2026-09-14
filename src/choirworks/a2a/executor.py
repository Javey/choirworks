from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from a2a.helpers import new_text_message, new_task
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    Role,
    Task,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict

from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.registry import AgentRegistry
from choirworks.core.llm import LiteLLMClient
from choirworks.core.planner import (
    PlanDraft,
    PlanNodeDraft,
    PlanningFailed,
    Planner,
    validate_plan,
)
from choirworks.core.policy import PolicyEngine

logger = logging.getLogger(__name__)

A2A_ROOM_URI = "https://github.com/Javey/choirworks/extensions/room/v1"

TERMINAL_NODE_STATUSES = {"completed", "failed", "canceled", "invalidated"}
WORKING_NODE_STATUSES = {"ready", "dispatched", "working"}


@dataclass
class NodeState:
    id: str
    name: str
    agent_name: str
    agent_url: str
    status: str = "pending"
    attempt: int = 0
    a2a_task_id: str | None = None
    output: str | None = None
    error: str | None = None
    deps: list[str] = field(default_factory=list)
    input_text: str = ""
    derived: bool = False
    policy_override: str | None = None


@dataclass
class PlanState:
    id: str
    version: int
    rationale: str
    nodes: dict[str, NodeState] = field(default_factory=dict)

    def ready_nodes(self) -> list[NodeState]:
        result = []
        for node in self.nodes.values():
            if node.status == "pending" and all(
                self.nodes[dep].status == "completed"
                for dep in node.deps
                if dep in self.nodes
            ):
                result.append(node)
        return result

    def all_completed(self) -> bool:
        active = [n for n in self.nodes.values() if n.status != "invalidated"]
        return bool(active) and all(n.status == "completed" for n in active)

    def has_failures(self) -> bool:
        return any(n.status == "failed" for n in self.nodes.values())


def _struct(data: dict[str, Any]) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    ParseDict(data, s)
    return s


def _status_update(
    task_id: str,
    context_id: str,
    state: TaskState,
    *,
    kind: str | None = None,
    **metadata: Any,
) -> TaskStatusUpdateEvent:
    meta: dict[str, Any] = {}
    if kind:
        meta["kind"] = kind
    meta.update(metadata)
    return TaskStatusUpdateEvent(
        task_id=task_id,
        context_id=context_id,
        status=TaskStatus(state=state),
        metadata=_struct(meta) if meta else None,
    )


def _join_text(parts: Any) -> str:
    return "\n".join(p.text for p in parts if p.HasField("text"))


_REMOTE_STATE_MAP: dict[int, str] = {
    TaskState.TASK_STATE_SUBMITTED: "dispatched",
    TaskState.TASK_STATE_WORKING: "working",
    TaskState.TASK_STATE_INPUT_REQUIRED: "input_required",
    TaskState.TASK_STATE_AUTH_REQUIRED: "input_required",
    TaskState.TASK_STATE_COMPLETED: "completed",
    TaskState.TASK_STATE_FAILED: "failed",
    TaskState.TASK_STATE_CANCELED: "canceled",
    TaskState.TASK_STATE_REJECTED: "failed",
    TaskState.TASK_STATE_UNSPECIFIED: "working",
}


class ChoirWorksAgentExecutor(AgentExecutor):
    """ChoirWorks orchestration engine as an A2A AgentExecutor.

    Each execute() call processes one user message:
    1. Post the user message
    2. Plan the DAG (if needed)
    3. Dispatch ready nodes to remote agents
    4. Forward remote events to the local event queue
    5. Handle interventions / failures
    6. Leave task in WORKING (more messages expected) or INPUT_REQUIRED
    """

    def __init__(
        self,
        registry: AgentRegistry,
        remote: RemoteAgentClient,
        planner: Planner,
        policy: PolicyEngine,
        llm: LiteLLMClient,
        *,
        max_parallel: int = 5,
        node_timeout: float = 600.0,
        max_node_attempts: int = 2,
        retry_backoff: float = 1.0,
        max_derived_nodes: int = 5,
    ):
        self._registry = registry
        self._remote = remote
        self._planner = planner
        self._policy = policy
        self._llm = llm
        self._max_parallel = max_parallel
        self._node_timeout = node_timeout
        self._max_node_attempts = max_node_attempts
        self._retry_backoff = retry_backoff
        self._max_derived_nodes = max_derived_nodes

    async def execute(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        text = context.get_user_input()
        task_id = context.task_id
        context_id = context.context_id

        if not text:
            return

        updater = TaskUpdater(event_queue, task_id, context_id)

        # Emit initial Task if this is a new task
        existing = context.current_task
        if existing is None:
            initial_task = new_task(
                task_id=task_id,
                context_id=context_id,
                state=TaskState.TASK_STATE_SUBMITTED,
                history=[context.message] if context.message else None,
            )
            await event_queue.enqueue_event(initial_task)

        # Start work
        await updater.start_work()

        # Resolve members from mentions
        mentions = self._extract_mentions(text)
        await self._join_members(event_queue, task_id, context_id, mentions)

        # Plan
        plan = await self._create_plan(text, task_id, context_id, event_queue)
        if plan is None:
            fail_msg = new_text_message(
                "Planning failed", role=Role.ROLE_AGENT,
                task_id=task_id, context_id=context_id,
            )
            await updater.failed(fail_msg)
            return

        # Dispatch + schedule loop
        await self._schedule_loop(plan, task_id, context_id, event_queue, updater)

        # Finalize
        if plan.all_completed():
            await self._announce_completion(plan, task_id, context_id, event_queue)
            await updater.complete()
        else:
            await updater.requires_input()

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        task_id = context.task_id
        context_id = context.context_id or ""
        # Cancel remote tasks for active nodes
        # (In a full implementation, we'd track active nodes and cancel them)
        await event_queue.enqueue_event(
            _status_update(task_id, context_id, TaskState.TASK_STATE_CANCELED,
                          kind="task.canceled")
        )

    async def _create_plan(
        self, text: str, task_id: str, context_id: str, event_queue: EventQueue
    ) -> PlanState | None:
        try:
            draft = await self._planner.plan(text)
        except PlanningFailed as exc:
            logger.warning("Planning failed: %s", exc)
            return None

        agents = await self._registry.list()
        agent_urls = {agent.name: agent.card_url for agent in agents}

        plan_id = f"plan-{uuid.uuid4().hex[:8]}"
        nodes: dict[str, NodeState] = {}
        for node_draft in draft.nodes:
            url = agent_urls.get(node_draft.agent_name, "")
            nodes[node_draft.id] = NodeState(
                id=node_draft.id,
                name=node_draft.name,
                agent_name=node_draft.agent_name,
                agent_url=url,
                deps=list(node_draft.deps),
                input_text=str(node_draft.input.get("text", "")),
                policy_override=node_draft.policy_override,
            )

        plan = PlanState(
            id=plan_id,
            version=1,
            rationale=draft.rationale,
            nodes=nodes,
        )

        # Emit plan created event
        await event_queue.enqueue_event(
            _status_update(
                task_id, context_id, TaskState.TASK_STATE_WORKING,
                kind="plan.created",
                plan_id=plan_id,
                plan_version=plan.version,
                rationale=plan.rationale,
                nodes=[
                    {
                        "id": n.id,
                        "name": n.name,
                        "agent_name": n.agent_name,
                        "deps": n.deps,
                    }
                    for n in plan.nodes.values()
                ],
            )
        )

        # Join plan members
        plan_agents = list({
            n.agent_name for n in plan.nodes.values() if n.agent_name
        })
        await self._join_members(event_queue, task_id, context_id, plan_agents)

        return plan

    async def _schedule_loop(
        self,
        plan: PlanState,
        task_id: str,
        context_id: str,
        event_queue: EventQueue,
        updater: TaskUpdater,
    ) -> None:
        pending_tasks: set[asyncio.Task] = set()

        for _ in range(100):
            ready = plan.ready_nodes()
            if ready:
                slots = self._max_parallel - len(pending_tasks)
                for node in ready[:max(0, slots)]:
                    node.status = "dispatched"
                    task = asyncio.create_task(
                        self._dispatch_node(node, plan, task_id, context_id, event_queue)
                    )
                    pending_tasks.add(task)
                    task.add_done_callback(pending_tasks.discard)

            if not pending_tasks:
                if plan.all_completed():
                    return
                if plan.has_failures():
                    await self._handle_failures(plan, task_id, context_id, event_queue)
                    continue
                if any(n.status == "input_required" for n in plan.nodes.values()):
                    return
                logger.warning("Schedule loop stalled for task %s", task_id)
                return

            done, pending_tasks = await asyncio.wait(
                pending_tasks, return_when=asyncio.FIRST_COMPLETED,
            )

            if any(n.status == "input_required" for n in plan.nodes.values()):
                await self._process_interventions(
                    plan, task_id, context_id, event_queue
                )

    async def _dispatch_node(
        self,
        node: NodeState,
        plan: PlanState,
        task_id: str,
        context_id: str,
        event_queue: EventQueue,
    ) -> None:
        node.attempt += 1

        # Emit dispatch intent
        await event_queue.enqueue_event(
            _status_update(
                task_id, context_id, TaskState.TASK_STATE_WORKING,
                kind="node.dispatch_intent",
                node_id=node.id,
                attempt=node.attempt,
            )
        )

        # Dispatch to remote agent
        artifacts: list[dict[str, Any]] = []
        current = "working"
        try:
            async with asyncio.timeout(self._node_timeout):
                current = await self._consume_remote(
                    node, task_id, context_id, event_queue, artifacts
                )
        except TimeoutError:
            node.error = f"node timed out after {self._node_timeout}s"
            node.status = "failed"
        except Exception as exc:
            node.error = str(exc)
            node.status = "failed"

        # Handle stream end
        if current == "completed":
            node.status = "completed"
            node.output = " ".join(
                a.get("text", "") for a in artifacts if a.get("text")
            ).strip() or None
            await event_queue.enqueue_event(
                _status_update(
                    task_id, context_id, TaskState.TASK_STATE_WORKING,
                    kind="node.completed",
                    node_id=node.id,
                    output_summary=(node.output or "")[:200],
                )
            )
        elif current == "input_required":
            node.status = "input_required"
            await event_queue.enqueue_event(
                _status_update(
                    task_id, context_id, TaskState.TASK_STATE_INPUT_REQUIRED,
                    kind="node.input_required",
                    node_id=node.id,
                    agent_name=node.agent_name,
                )
            )
        elif node.status == "failed":
            await event_queue.enqueue_event(
                _status_update(
                    task_id, context_id, TaskState.TASK_STATE_WORKING,
                    kind="node.failed",
                    node_id=node.id,
                    error=node.error or "unknown error",
                )
            )

    async def _consume_remote(
        self,
        node: NodeState,
        task_id: str,
        context_id: str,
        event_queue: EventQueue,
        artifacts: list[dict[str, Any]],
    ) -> str:
        text = node.input_text or str(node.id)
        current = "dispatched"

        async for chunk in self._remote.send_text(
            node.agent_url, text, context_id=context_id,
            message_id=f"{task_id}:{node.id}:{node.attempt}",
        ):
            if chunk.HasField("task"):
                node.a2a_task_id = chunk.task.id
                node.status = "dispatched"
                current = "dispatched"
                await event_queue.enqueue_event(
                    _status_update(
                        task_id, context_id, TaskState.TASK_STATE_WORKING,
                        kind="node.dispatched",
                        node_id=node.id,
                        a2a_task_id=chunk.task.id,
                    )
                )
            elif chunk.HasField("status_update"):
                state = chunk.status_update.status.state
                mapped = _REMOTE_STATE_MAP.get(state)
                if mapped and mapped != current:
                    current = mapped
                    if mapped == "input_required" and chunk.status_update.status.HasField("message"):
                        question = _join_text(chunk.status_update.status.message.parts)
                        await event_queue.enqueue_event(
                            _status_update(
                                task_id, context_id, TaskState.TASK_STATE_INPUT_REQUIRED,
                                kind="intervention.question",
                                node_id=node.id,
                                agent_name=node.agent_name,
                                question=question,
                            )
                        )
            elif chunk.HasField("artifact_update"):
                update = chunk.artifact_update
                piece = _join_text(update.artifact.parts)
                append = bool(update.append)
                entry = next(
                    (a for a in artifacts if a["id"] == update.artifact.artifact_id),
                    None,
                )
                if append and entry:
                    entry["text"] += piece
                else:
                    merged = {
                        "id": update.artifact.artifact_id,
                        "name": update.artifact.name,
                        "text": piece,
                    }
                    if entry:
                        artifacts[artifacts.index(entry)] = merged
                    else:
                        artifacts.append(merged)

                # Forward artifact chunk (partial if not last_chunk)
                art = Artifact(
                    artifact_id=f"{node.id}:{update.artifact.artifact_id}",
                    name=update.artifact.name or node.name,
                    parts=[Part(text=piece)],
                )
                await event_queue.enqueue_event(
                    TaskArtifactUpdateEvent(
                        task_id=task_id,
                        context_id=context_id,
                        artifact=art,
                        append=append,
                        last_chunk=bool(update.last_chunk),
                        metadata=_struct({
                            "node_id": node.id,
                            "agent_name": node.agent_name,
                        }),
                    )
                )
            elif chunk.HasField("message"):
                msg_text = _join_text(chunk.message.parts)
                artifacts.append({"id": "message", "name": "message", "text": msg_text})
                # Post agent message via status_update (not bare Message, which is
                # disallowed in task mode)
                agent_msg = new_text_message(
                    msg_text, role=Role.ROLE_AGENT,
                    task_id=task_id, context_id=context_id,
                )
                ParseDict({A2A_ROOM_URI: {
                    "sender": node.agent_name,
                    "node_id": node.id,
                    "role": "agent",
                }}, agent_msg.metadata)
                await event_queue.enqueue_event(
                    TaskStatusUpdateEvent(
                        task_id=task_id,
                        context_id=context_id,
                        status=TaskStatus(
                            state=TaskState.TASK_STATE_WORKING,
                            message=agent_msg,
                        ),
                        metadata=_struct({"kind": "agent.message", "node_id": node.id}),
                    )
                )

        return current

    async def _handle_failures(
        self, plan: PlanState, task_id: str, context_id: str, event_queue: EventQueue
    ) -> None:
        for node in plan.nodes.values():
            if node.status != "failed":
                continue
            if node.attempt < self._max_node_attempts:
                # Retry
                node.status = "pending"
                node.error = None
                await event_queue.enqueue_event(
                    _status_update(
                        task_id, context_id, TaskState.TASK_STATE_WORKING,
                        kind="node.retry_scheduled",
                        node_id=node.id,
                        attempt=node.attempt + 1,
                    )
                )
            else:
                logger.warning(
                    "Node %s exhausted retries (%d attempts)", node.id, node.attempt
                )

    async def _process_interventions(
        self, plan: PlanState, task_id: str, context_id: str, event_queue: EventQueue
    ) -> None:
        for node in plan.nodes.values():
            if node.status != "input_required":
                continue
            policy = self._policy.resolve(
                node.policy_override, node.agent_name, None, None
            )
            if policy == "human":
                # Leave as input_required - wait for human response
                continue
            if policy == "auto_llm":
                # Try to auto-answer
                question = node.output or ""
                try:
                    answer = await self._llm.text(
                        system="You are helping resolve a question during task execution.",
                        user=question,
                    )
                    # Re-dispatch with the answer
                    node.status = "pending"
                    node.input_text = answer
                except Exception:
                    logger.warning("Auto-LLM intervention failed for node %s", node.id)
            # peer_agent: would dispatch to another agent (simplified)

    async def _join_members(
        self, event_queue: EventQueue, task_id: str, context_id: str,
        names: list[str],
    ) -> None:
        records = await self._registry.list()
        known = {r.name: r for r in records}
        for name in names:
            if name not in known:
                continue
            record = known[name]
            await event_queue.enqueue_event(
                _status_update(
                    task_id, context_id, TaskState.TASK_STATE_WORKING,
                    kind="room.participant_joined",
                    agent_name=name,
                    agent_url=record.card_url,
                    reason="human_mention",
                )
            )

    def _extract_mentions(self, text: str) -> list[str]:
        import re
        records = asyncio.get_event_loop()
        # Simple regex extraction - will be validated against registry
        return re.findall(r"@([A-Za-z0-9_-]+)", text)

    async def _announce_completion(
        self, plan: PlanState, task_id: str, context_id: str, event_queue: EventQueue
    ) -> None:
        summaries = []
        for node in plan.nodes.values():
            if node.status == "completed":
                summary = node.output or "(no output)"
                summaries.append(f"- {node.agent_name or node.name}: {summary[:80]}")
        if summaries:
            completion_msg = new_text_message(
                "任务完成：\n" + "\n".join(summaries),
                role=Role.ROLE_AGENT,
                task_id=task_id,
                context_id=context_id,
            )
            ParseDict({A2A_ROOM_URI: {
                "sender": "assistant",
                "role": "assistant",
                "kind": "task_completion",
            }}, completion_msg.metadata)
            await event_queue.enqueue_event(
                TaskStatusUpdateEvent(
                    task_id=task_id,
                    context_id=context_id,
                    status=TaskStatus(
                        state=TaskState.TASK_STATE_WORKING,
                        message=completion_msg,
                    ),
                    metadata=_struct({"kind": "task.completion_summary"}),
                )
            )
