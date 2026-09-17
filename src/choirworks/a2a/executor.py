from __future__ import annotations

import asyncio
import logging
import re
import uuid
from collections.abc import Sequence
from typing import Any, Literal

from a2a.helpers import new_task, new_text_message
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    Role,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict
from pydantic import BaseModel, create_model

from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.registry import AgentRegistry
from choirworks.a2a.room import A2A_ROOM_URI, RoomOptions, room_options
from choirworks.a2a.state import (
    ACTIVE_NODE_STATUSES,
    NodeState,
    OrchestrationState,
    load_state,
)
from choirworks.core.context import (
    ContextBriefBuilder,
    build_assist_input,
    build_assistance_decision_user,
    build_followup_input,
    build_peer_fallback_input,
    build_replan_context,
    build_replan_reason,
)
from choirworks.core.llm import LiteLLMClient
from choirworks.core.planner import Planner, PlanningFailed

logger = logging.getLogger(__name__)


class AssistanceDecision(BaseModel):
    action: Literal["peer", "human"]
    agent_name: str | None = None
    instruction: str = ""
    reasoning: str = ""


def assistance_decision_schema(
    candidate_names: Sequence[str],
) -> type[AssistanceDecision]:
    fields: dict[str, Any] = {}
    if candidate_names:
        fields["agent_name"] = (Literal[*candidate_names] | None, None)
    else:
        fields["action"] = (Literal["human"], ...)
    return create_model("AssistanceDecision", __base__=AssistanceDecision, **fields)


ASSISTANCE_SYSTEM = """You are the orchestrator of a multi-agent group.
An agent is blocked and needs help. Decide how to handle it:
- action="peer": delegate to another registered agent that can help
- action="human": escalate to a human

Return only JSON matching the schema.
When action="peer", agent_name must be one of the listed candidates
and instruction should describe the task.
When action="human", leave agent_name empty.
- reasoning: one short sentence explaining your decision."""


def _struct(data: dict[str, Any]) -> struct_pb2.Struct:
    result = struct_pb2.Struct()
    ParseDict(_strip_none(data), result)
    return result


def _strip_none(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_none(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list):
        return [_strip_none(item) for item in value]
    return value


def _status_update(
    task_id: str,
    context_id: str,
    state: TaskState,
    *,
    kind: str | None = None,
    orch_state: OrchestrationState | None = None,
    message: Message | None = None,
    **metadata: Any,
) -> TaskStatusUpdateEvent:
    meta: dict[str, Any] = {}
    if kind:
        meta["kind"] = kind
    meta.update(metadata)
    if orch_state is not None:
        meta.update(orch_state.to_dict())
    return TaskStatusUpdateEvent(
        task_id=task_id,
        context_id=context_id,
        status=TaskStatus(
            state=state,
            message=message if message is not None else None,
        ),
        metadata=_struct(meta) if meta else None,
    )


def _join_text(parts: Any) -> str:
    return "\n".join(p.text for p in parts if p.HasField("text"))


def _is_resume_message(message: Message | None) -> bool:
    if message is None or not message.metadata.fields:
        return False
    return "choirworks.resume" in message.metadata.fields


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

    The A2A Task is the aggregate root. Plan/node/member/intervention state is
    persisted as A2A event metadata merged into the Task snapshot by the SDK
    ``TaskManager`` + ``DatabaseTaskStore``.

    ``execute()`` routes each inbound message and returns quickly; node work
    runs in background runners, so multiple agents can work and chat at once.
    """

    def __init__(
        self,
        registry: AgentRegistry,
        remote: RemoteAgentClient,
        planner: Planner,
        llm: LiteLLMClient,
        *,
        max_parallel: int = 5,
        node_timeout: float = 600.0,
        max_node_attempts: int = 2,
        retry_backoff: float = 1.0,
        max_derived_nodes: int = 5,
        replan_on_failure: bool = True,
        compaction_threshold: float = 0.8,
        compaction_retention: int = 10,
    ):
        self._registry = registry
        self._remote = remote
        self._planner = planner
        self._llm = llm
        self._max_parallel = max_parallel
        self._node_timeout = node_timeout
        self._max_node_attempts = max_node_attempts
        self._retry_backoff = retry_backoff
        self._max_derived_nodes = max_derived_nodes
        self._replan_on_failure = replan_on_failure
        self._brief_builder = ContextBriefBuilder(
            llm,
            compaction_threshold=compaction_threshold,
            compaction_retention=compaction_retention,
        )

        self._states: dict[str, OrchestrationState] = {}
        self._runners: dict[str, asyncio.Task] = {}
        self._queues: dict[str, EventQueue] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._node_tasks: dict[str, dict[asyncio.Task, NodeState]] = {}
        self._context_ids: dict[str, str] = {}

    # ------------------------------------------------------------- lifecycle

    def set_task_store(self, task_store: Any) -> None:
        """Injected by the app so follow-up plans can read conversation history."""
        self._brief_builder.set_task_store(task_store)

    def _lock_for(self, task_id: str) -> asyncio.Lock:
        return self._locks.setdefault(task_id, asyncio.Lock())

    async def execute(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        text = (context.get_user_input() or "").strip()
        task_id = context.task_id or ""
        context_id = context.context_id or ""
        existing = context.current_task
        room = room_options(context.message)
        mentions = list(room.get("mentions") or [])
        for name in re.findall(r"@([A-Za-z0-9_-]+)", text):
            if name not in mentions:
                mentions.append(name)
        if mentions:
            room["mentions"] = mentions

        if existing is None:
            initial_task = new_task(
                task_id=task_id,
                context_id=context_id,
                state=TaskState.TASK_STATE_SUBMITTED,
                history=[context.message] if context.message else None,
            )
            await event_queue.enqueue_event(initial_task)

        updater = TaskUpdater(event_queue, task_id, context_id)
        self._queues[task_id] = event_queue
        self._context_ids[task_id] = context_id

        async with self._lock_for(task_id):
            state = self._states.get(task_id)
            if state is None:
                state = load_state(existing)
                if state is not None:
                    self._states[task_id] = state

            if state is None:
                if not text:
                    return
                state = OrchestrationState(plan_id=self._new_plan_id())
                self._states[task_id] = state
                await updater.start_work()
                await self._plan_and_launch(
                    state, text, task_id, context_id, event_queue, room=room
                )
                return

            if state.pending_interventions():
                await self._answer_intervention(
                    state, text, task_id, context_id, event_queue
                )
                return

            if _is_resume_message(context.message):
                await self._resume(state, task_id, context_id, event_queue)
                return

            if state.has_pending_work():
                await self._route_message(
                    state, text, task_id, context_id, event_queue, room
                )
                return

            # Everything settled: treat the message as a follow-up request.
            if not text:
                return
            await updater.start_work()
            await self._plan_and_launch(
                state, text, task_id, context_id, event_queue, room=room, follow_up=True
            )

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        task_id = context.task_id or ""
        context_id = context.context_id or ""
        async with self._lock_for(task_id):
            state = self._states.get(task_id)
            runner = self._runners.pop(task_id, None)
            if runner is not None:
                runner.cancel()
            if state is not None:
                for node in list(state.nodes.values()):
                    if node.status in ACTIVE_NODE_STATUSES | {"ready"}:
                        node.status = "canceled"
                # Emit the terminal event first: the SDK closes the agent event
                # queue as soon as the cancelled producer unwinds.
                await self._emit(
                    event_queue,
                    state,
                    task_id,
                    context_id,
                    "task.canceled",
                    TaskState.TASK_STATE_CANCELED,
                )
                for node in list(state.nodes.values()):
                    if node.status == "canceled" and node.a2a_task_id:
                        await self._remote.cancel_task(
                            node.agent_url, node.a2a_task_id
                        )
            else:
                await event_queue.enqueue_event(
                    _status_update(
                        task_id,
                        context_id,
                        TaskState.TASK_STATE_CANCELED,
                        kind="task.canceled",
                    )
                )
        self._states.pop(task_id, None)
        self._queues.pop(task_id, None)

    async def shutdown(self) -> None:
        for runner in list(self._runners.values()):
            runner.cancel()
        for runner in list(self._runners.values()):
            try:
                await runner
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._runners.clear()

    async def _resume(
        self,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
        event_queue: EventQueue,
    ) -> None:
        """Re-attach to remote work after a process restart."""
        for node in state.nodes.values():
            if node.status in ACTIVE_NODE_STATUSES:
                node.status = "resume" if node.a2a_task_id else "pending"
        await self._persist(event_queue, state, task_id, context_id)
        self._start_runner(task_id)

    # ---------------------------------------------------------------- plan

    def _new_plan_id(self) -> str:
        return f"plan-{uuid.uuid4().hex[:8]}"

    async def _plan_and_launch(
        self,
        state: OrchestrationState,
        text: str,
        task_id: str,
        context_id: str,
        event_queue: EventQueue,
        *,
        room: RoomOptions | None = None,
        follow_up: bool = False,
    ) -> None:
        context_brief = await self._brief_builder.build(
            context_id, exclude_task_id=task_id
        )
        try:
            draft, reasoning = await self._planner.plan_with_reasoning(
                text, context=context_brief or None
            )
        except PlanningFailed as exc:
            logger.warning("Planning failed for task %s: %s", task_id, exc)
            message = new_text_message(
                f"规划失败：{exc}", role=Role.ROLE_AGENT,
                task_id=task_id, context_id=context_id,
            )
            await self._emit(
                event_queue, state, task_id, context_id,
                "plan.failed", TaskState.TASK_STATE_FAILED, message=message,
            )
            return

        agents = await self._registry.list()
        agent_urls = {agent.name: agent.card_url for agent in agents}
        if follow_up:
            state.plan_version += 1
            # Keep terminal nodes for history; start a fresh node set for the
            # follow-up so deps never point at removed nodes.
            state.nodes = {}
            state.queue = {}
        state.plan_id = self._new_plan_id()
        state.rationale = draft.rationale

        for node_draft in draft.nodes:
            node = NodeState(
                id=node_draft.id,
                name=node_draft.name,
                agent_name=node_draft.agent_name,
                agent_url=agent_urls.get(node_draft.agent_name, ""),
                deps=list(node_draft.deps),
                input_text=str(node_draft.input.get("text", "")),
            )
            state.nodes[node.id] = node

        if reasoning:
            reasoning_msg = new_text_message(
                reasoning, role=Role.ROLE_AGENT,
                task_id=task_id, context_id=context_id,
            )
            ParseDict(
                {
                    A2A_ROOM_URI: {
                        "sender": "assistant",
                        "role": "assistant",
                        "kind": "assistant.reasoning",
                    }
                },
                reasoning_msg.metadata,
            )
            ParseDict({"cw_thought": True}, reasoning_msg.parts[0].metadata)
            await self._emit(
                event_queue, state, task_id, context_id,
                "assistant.reasoning", TaskState.TASK_STATE_WORKING,
                message=reasoning_msg,
            )

        await self._emit(
            event_queue, state, task_id, context_id,
            "plan.created", TaskState.TASK_STATE_WORKING,
            plan_id=state.plan_id,
            plan_version=state.plan_version,
            rationale=state.rationale,
            nodes=[
                {
                    "id": n.id,
                    "name": n.name,
                    "agent_name": n.agent_name,
                    "deps": n.deps,
                }
                for n in state.nodes.values()
            ],
        )
        plan_text = "任务已拆解：\n" + "\n".join(
            f"- @{n.agent_name or n.id} 负责 {n.name}" for n in state.nodes.values()
        )
        await self._emit_room_message(
            event_queue, state, task_id, context_id, "plan.announced", plan_text
        )
        await self._join_members(
            state,
            [n.agent_name for n in state.nodes.values() if n.agent_name],
            "plan",
            task_id,
            context_id,
            event_queue,
        )
        mention_targets = [
            name
            for name in (room or {}).get("mentions", [])
            if name in agent_urls
        ]
        if mention_targets:
            await self._join_members(
                state, mention_targets, "human_mention", task_id, context_id, event_queue
            )
        await self._persist(event_queue, state, task_id, context_id)
        self._start_runner(task_id)

    # --------------------------------------------------------------- runner

    def _start_runner(self, task_id: str) -> None:
        existing = self._runners.get(task_id)
        if existing is not None and not existing.done():
            return
        state = self._states.get(task_id)
        if state is None:
            return
        self._runners[task_id] = asyncio.create_task(
            self._run_plan(task_id), name=f"choirworks-runner:{task_id}"
        )

    async def _run_plan(self, task_id: str) -> None:
        state = self._states[task_id]
        try:
            while True:
                async with self._lock_for(task_id):
                    state = self._states.get(task_id)
                    if state is None:
                        return
                    queue = self._queues[task_id]
                    context_id = self._context_ids.get(task_id, "")

                    for node in list(state.failed_nodes()):
                        if node.attempt < self._max_node_attempts:
                            node.status = "pending"
                            node.error = None
                            await self._emit(
                                queue, state, task_id, context_id,
                                "node.retry_scheduled",
                                node_id=node.id,
                                attempt=node.attempt,
                            )

                    ready = state.ready_nodes()
                    slots = max(0, self._max_parallel - self._pending_count(task_id))
                    for node in ready[:slots]:
                        mode = (
                            "resume"
                            if node.status == "resume"
                            else ("continue" if node.status == "ready" else "dispatch")
                        )
                        if mode != "resume":
                            node.status = "dispatched"
                        node_task = asyncio.create_task(
                            self._execute_node(
                                node, state, task_id, context_id, mode=mode
                            ),
                            name=f"choirworks-node:{task_id}:{node.id}",
                        )
                        self._node_tasks.setdefault(task_id, {})[node_task] = node

                pending = self._node_tasks.get(task_id, {})
                if pending:
                    done, _ = await asyncio.wait(
                        set(pending), return_when=asyncio.FIRST_COMPLETED
                    )
                    for finished in done:
                        node = pending.pop(finished)
                        exception = finished.exception()
                        if exception is not None:
                            node.status = "failed"
                            node.error = str(exception)
                            await self._emit(
                                queue, state, task_id, context_id,
                                "node.failed", node_id=node.id, error=node.error,
                            )
                    if self._retry_backoff > 0 and any(
                        n.status == "failed" and n.attempt < self._max_node_attempts
                        for n in state.nodes.values()
                    ):
                        await asyncio.sleep(self._retry_backoff)
                    continue

                async with self._lock_for(task_id):
                    state = self._states.get(task_id)
                    if state is None:
                        return
                    queue = self._queues[task_id]
                    if any(
                        n.status == "failed" and n.attempt < self._max_node_attempts
                        for n in state.nodes.values()
                    ):
                        continue
                    if state.input_required_nodes():
                        progress = await self._settle_input(
                            state, task_id, context_id, queue
                        )
                        if progress:
                            continue
                        await self._emit(
                            queue, state, task_id, context_id,
                            "task.requires_input",
                            TaskState.TASK_STATE_INPUT_REQUIRED,
                        )
                        return
                    if state.all_completed():
                        await self._emit(
                            queue, state, task_id, context_id,
                            "task.completed", TaskState.TASK_STATE_COMPLETED,
                        )
                        self._states.pop(task_id, None)
                        return
                    if state.has_failures():
                        recovered = False
                        if self._replan_on_failure:
                            recovered = await self._replan(
                                state, task_id, context_id, queue
                            )
                        if recovered:
                            continue
                        failed = new_text_message(
                            "任务失败", role=Role.ROLE_AGENT,
                            task_id=task_id, context_id=context_id,
                        )
                        await self._emit(
                            queue, state, task_id, context_id,
                            "task.failed", TaskState.TASK_STATE_FAILED,
                            message=failed,
                        )
                        self._states.pop(task_id, None)
                        return
                    if state.has_pending_work():
                        schedulable = bool(state.ready_nodes()) or (
                            self._pending_count(task_id) > 0
                        )
                        if schedulable:
                            await asyncio.sleep(0)
                            continue
                        logger.warning(
                            "Runner stalled for task %s: %s",
                            task_id,
                            {node.id: node.status for node in state.nodes.values()},
                        )
                    else:
                        logger.warning("Runner stalled for task %s", task_id)
                    failed = new_text_message(
                        "任务停滞", role=Role.ROLE_AGENT,
                        task_id=task_id, context_id=context_id,
                    )
                    await self._emit(
                        queue, state, task_id, context_id,
                        "task.failed", TaskState.TASK_STATE_FAILED,
                        message=failed,
                    )
                    self._states.pop(task_id, None)
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - never let the runner die silently
            logger.exception("Runner failed for task %s", task_id)
            state = self._states.get(task_id)
            queue = self._queues.get(task_id)
            if state is not None and queue is not None:
                failed = new_text_message(
                    f"任务失败：{exc}", role=Role.ROLE_AGENT,
                    task_id=task_id, context_id=self._context_ids.get(task_id, ""),
                )
                try:
                    await self._emit(
                        queue, state, task_id, self._context_ids.get(task_id, ""),
                        "task.failed", TaskState.TASK_STATE_FAILED,
                        message=failed,
                    )
                except Exception:  # noqa: BLE001 - queue may already be closed
                    logger.exception("Failed to emit task failure for %s", task_id)
                self._states.pop(task_id, None)
        finally:
            self._runners.pop(task_id, None)
            for node_task in list(self._node_tasks.pop(task_id, {})):
                node_task.cancel()

    def _pending_count(self, task_id: str) -> int:
        return len(self._node_tasks.get(task_id, {}))

    async def _execute_node(
        self,
        node: NodeState,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
        *,
        mode: str = "dispatch",
    ) -> None:
        queue = self._queues[task_id]
        continuation = mode == "continue"
        if mode != "resume":
            node.attempt += 1
        if mode == "dispatch":
            await self._emit(
                queue, state, task_id, context_id,
                "node.dispatch_intent",
                node_id=node.id,
                attempt=node.attempt,
            )
        elif mode == "resume":
            await self._emit(
                queue, state, task_id, context_id,
                "node.resumed",
                node_id=node.id,
                a2a_task_id=node.a2a_task_id,
            )
        current = "working"
        try:
            async with asyncio.timeout(self._node_timeout):
                if mode == "resume":
                    current = await self._resume_remote(
                        node, state, task_id, context_id, queue
                    )
                else:
                    current = await self._stream_remote(
                        node,
                        node.input_text,
                        state,
                        task_id,
                        context_id,
                        queue,
                        continuation=continuation,
                    )
        except TimeoutError:
            node.error = f"node timed out after {self._node_timeout}s"
            node.status = "failed"
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - remote agent failures are node failures
            node.error = str(exc)
            node.status = "failed"

        if current == "completed":
            node.status = "completed"
            await self._emit(
                queue, state, task_id, context_id,
                "node.completed",
                node_id=node.id,
                agent_name=node.agent_name,
                output_summary=(node.output or "")[:200],
            )
            await self._arbitrate_mentions(state, node, task_id, context_id, queue)
            await self._deliver_queued(state, node, task_id, context_id, queue)
        elif current == "input_required":
            node.status = "input_required"
            await self._emit(
                queue, state, task_id, context_id,
                "node.input_required",
                TaskState.TASK_STATE_INPUT_REQUIRED,
                node_id=node.id,
                agent_name=node.agent_name,
                question=node.question or "",
            )
        else:
            node.status = "failed"
            logger.warning(
                "Node %s (%s) failed: %s", node.id, node.agent_name, node.error
            )
            await self._emit(
                queue, state, task_id, context_id,
                "node.failed",
                node_id=node.id,
                error=node.error or "unknown error",
            )

    async def _stream_remote(
        self,
        node: NodeState,
        text: str,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
        queue: EventQueue,
        *,
        continuation: bool,
    ) -> str:
        remote_task_id = node.a2a_task_id if continuation else None
        chunks = self._remote.send_text(
            node.agent_url,
            text,
            task_id=remote_task_id,
            context_id=context_id,
            message_id=f"{task_id}:{node.id}:{node.attempt}",
        )
        current = await self._consume_chunks(
            node, state, task_id, context_id, queue, chunks
        )
        return await self._ensure_terminal(
            node, state, task_id, context_id, queue, current
        )

    async def _resume_remote(
        self,
        node: NodeState,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
        queue: EventQueue,
    ) -> str:
        if not node.a2a_task_id:
            return "failed"
        current = "working"
        try:
            chunks = self._remote.subscribe_task(node.agent_url, node.a2a_task_id)
            current = await self._consume_chunks(
                node, state, task_id, context_id, queue, chunks
            )
        except Exception as exc:  # noqa: BLE001 - task may already be terminal
            logger.debug("Resume subscribe failed for %s: %s", node.id, exc)
        return await self._ensure_terminal(
            node, state, task_id, context_id, queue, current
        )

    async def _ensure_terminal(
        self,
        node: NodeState,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
        queue: EventQueue,
        current: str,
    ) -> str:
        """Follow a remote task until it settles.

        A ChoirWorks peer returns control from ``execute()`` as soon as work is
        dispatched, so the send stream may end while the remote task is still
        running. In that case we poll the snapshot and subscribe to its updates.
        """
        settled = {"completed", "failed", "canceled", "input_required"}
        while current not in settled:
            if not node.a2a_task_id:
                return current
            task = await self._remote.get_task(node.agent_url, node.a2a_task_id)
            if task is not None:
                mapped = _REMOTE_STATE_MAP.get(task.status.state, current)
                if task.artifacts and mapped == "completed":
                    text = " ".join(
                        _join_text(artifact.parts)
                        for artifact in task.artifacts
                        if _join_text(artifact.parts)
                    ).strip()
                    if text:
                        node.output = text
                if (
                    mapped == "input_required"
                    and task.status.HasField("message")
                ):
                    node.question = _join_text(task.status.message.parts)
                current = mapped
                if current in settled:
                    return current
            try:
                chunks = self._remote.subscribe_task(
                    node.agent_url, node.a2a_task_id
                )
                current = await self._consume_chunks(
                    node, state, task_id, context_id, queue, chunks
                )
            except Exception as exc:  # noqa: BLE001 - retry via snapshot
                logger.debug("Follow subscribe failed for %s: %s", node.id, exc)
                await asyncio.sleep(0.2)
        return current

    async def _consume_chunks(
        self,
        node: NodeState,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
        queue: EventQueue,
        chunks: Any,
    ) -> str:
        artifacts: list[dict[str, Any]] = []
        current = "working"
        async for chunk in chunks:
            if chunk.HasField("task"):
                task = chunk.task
                node.a2a_task_id = task.id or node.a2a_task_id
                mapped = _REMOTE_STATE_MAP.get(task.status.state)
                if mapped:
                    current = mapped
                if task.artifacts:
                    artifacts[:] = [
                        {
                            "id": artifact.artifact_id,
                            "name": artifact.name,
                            "text": _join_text(artifact.parts),
                        }
                        for artifact in task.artifacts
                    ]
                node.status = "dispatched" if current == "dispatched" else node.status
                await self._emit(
                    queue, state, task_id, context_id,
                    "node.dispatched",
                    node_id=node.id,
                    a2a_task_id=node.a2a_task_id,
                )
            elif chunk.HasField("status_update"):
                state = chunk.status_update.status.state
                mapped = _REMOTE_STATE_MAP.get(state)
                if mapped and mapped != current:
                    current = mapped
                    if (
                        mapped == "input_required"
                        and chunk.status_update.status.HasField("message")
                    ):
                        node.question = _join_text(
                            chunk.status_update.status.message.parts
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
                art = Artifact(
                    artifact_id=f"{node.id}:{update.artifact.artifact_id}",
                    name=update.artifact.name or node.name,
                    parts=[Part(text=piece)],
                )
                await queue.enqueue_event(
                    TaskArtifactUpdateEvent(
                        task_id=task_id,
                        context_id=context_id,
                        artifact=art,
                        append=append,
                        last_chunk=bool(update.last_chunk),
                        metadata=_struct(
                            {
                                "kind": "node.artifact",
                                "node_id": node.id,
                                "agent_name": node.agent_name,
                            }
                        ),
                    )
                )
            elif chunk.HasField("message"):
                msg_text = _join_text(chunk.message.parts)
                artifacts.append({"id": "message", "name": "message", "text": msg_text})
                art = Artifact(
                    artifact_id=f"{node.id}:message",
                    name=node.name,
                    parts=[Part(text=msg_text)],
                )
                await queue.enqueue_event(
                    TaskArtifactUpdateEvent(
                        task_id=task_id,
                        context_id=context_id,
                        artifact=art,
                        append=False,
                        last_chunk=True,
                        metadata=_struct(
                            {
                                "kind": "agent.message",
                                "node_id": node.id,
                                "agent_name": node.agent_name,
                            }
                        ),
                    )
                )

        node.output = " ".join(
            artifact.get("text", "") for artifact in artifacts if artifact.get("text")
        ).strip() or None
        return current

    # -------------------------------------------------------- interventions

    async def _settle_input(
        self,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
        queue: EventQueue,
    ) -> bool:
        progress = False
        for node in list(state.input_required_nodes()):
            intervention = state.pending_intervention_for(node.id)
            if intervention is not None:
                continue
            helpers = [
                n
                for n in state.nodes.values()
                if n.derived
                and n.assist_requested_by == node.id
                and n.status == "completed"
            ]
            if helpers:
                helper = helpers[0]
                intervention = state.pending_intervention_for(node.id)
                if intervention is None:
                    intervention = state.add_intervention(
                        node.id, node.question or "需要协助"
                    )
                intervention.status = "resolved"
                intervention.answer = helper.output
                intervention.responder = helper.id
                node.input_text = helper.output or "(协助完成，无输出)"
                node.status = "ready"
                node.question = None
                await self._emit(
                    queue, state, task_id, context_id,
                    "intervention.resolved",
                    intervention_id=intervention.id,
                    node_id=node.id,
                    responder=helper.id,
                )
                progress = True
                continue
            active_helpers = [
                n
                for n in state.nodes.values()
                if n.derived
                and n.assist_requested_by == node.id
                and n.status in ACTIVE_NODE_STATUSES | {"pending", "ready"}
            ]
            if active_helpers:
                continue

            decision = await self._decide_assistance(node)
            if decision is not None and decision.action == "peer" and decision.agent_name:
                if await self._spawn_assist(
                    state, node, decision, task_id, context_id, queue
                ):
                    progress = True
                else:
                    await self._request_human(
                        state, node, task_id, context_id, queue
                    )
            else:
                await self._request_human(state, node, task_id, context_id, queue)
        return progress

    async def _request_human(
        self,
        state: OrchestrationState,
        node: NodeState,
        task_id: str,
        context_id: str,
        queue: EventQueue,
    ) -> None:
        intervention = state.add_intervention(node.id, node.question or "需要确认")
        question_msg = new_text_message(
            f"@{node.agent_name} 需要确认：{intervention.question}",
            role=Role.ROLE_AGENT,
            task_id=task_id,
            context_id=context_id,
        )
        question_msg.message_id = intervention.id
        ParseDict(
            {
                A2A_ROOM_URI: {
                    "sender": "assistant",
                    "role": "assistant",
                    "kind": "intervention.question",
                }
            },
            question_msg.metadata,
        )
        await self._emit(
            queue, state, task_id, context_id,
            "intervention.requested",
            TaskState.TASK_STATE_INPUT_REQUIRED,
            message=question_msg,
            intervention_id=intervention.id,
            node_id=node.id,
            agent_name=node.agent_name,
            question=intervention.question,
        )

    async def _decide_assistance(
        self, node: NodeState
    ) -> AssistanceDecision | None:
        if node.question is None:
            return None
        agents = await self._registry.list()
        candidates = [agent for agent in agents if agent.name != node.agent_name]
        if not candidates:
            return AssistanceDecision(action="human")
        user = build_assistance_decision_user(
            node.agent_name,
            node.question or node.input_text,
            candidates,
        )
        schema = assistance_decision_schema([agent.name for agent in candidates])
        try:
            return await self._llm.structured(
                system=ASSISTANCE_SYSTEM, user=user, schema=schema
            )
        except Exception:  # noqa: BLE001 - fall back to human
            logger.exception("assistance decision failed for %s", node.id)
            return AssistanceDecision(action="human")

    async def _spawn_assist(
        self,
        state: OrchestrationState,
        node: NodeState,
        decision: AssistanceDecision,
        task_id: str,
        context_id: str,
        queue: EventQueue,
    ) -> bool:
        if state.derived_count >= self._max_derived_nodes:
            return False
        agents = await self._registry.list()
        agent = next(
            (item for item in agents if item.name == decision.agent_name), None
        )
        if agent is None:
            return False
        state.derived_count += 1
        helper_id = f"{node.id}-h{state.derived_count}"
        helper = NodeState(
            id=helper_id,
            name=f"协助 · {node.name}",
            agent_name=agent.name,
            agent_url=agent.card_url,
            deps=[],
            input_text=decision.instruction
            or build_peer_fallback_input(node.question or node.input_text),
            derived=True,
            assist_requested_by=node.id,
        )
        state.nodes[helper_id] = helper
        await self._join_members(
            state, [agent.name], "peer_assist", task_id, context_id, queue
        )
        await self._emit_room_message(
            queue,
            state,
            task_id,
            context_id,
            "assist.dispatched",
            f"@{node.agent_name} 请求 @{agent.name} 协助，已加入工作",
        )
        await self._persist(queue, state, task_id, context_id)
        return True

    async def _answer_intervention(
        self,
        state: OrchestrationState,
        text: str,
        task_id: str,
        context_id: str,
        event_queue: EventQueue,
    ) -> None:
        pending = state.pending_interventions()
        if not pending or not text:
            return
        intervention = pending[0]
        intervention.status = "resolved"
        intervention.answer = text
        intervention.responder = "human"
        node = state.nodes.get(intervention.node_id)
        if node is not None:
            node.input_text = text
            node.question = None
            node.status = "ready"
        await self._emit(
            event_queue, state, task_id, context_id,
            "intervention.resolved",
            node_id=intervention.node_id,
            intervention_id=intervention.id,
            responder="human",
        )
        self._start_runner(task_id)

    # -------------------------------------------------------------- routing

    async def _route_message(
        self,
        state: OrchestrationState,
        text: str,
        task_id: str,
        context_id: str,
        event_queue: EventQueue,
        room: RoomOptions,
    ) -> None:
        if not text:
            return
        active = state.active_nodes()
        quote_id = room.get("quote_id")
        interrupt = bool(room.get("interrupt"))

        if quote_id and not interrupt:
            quoted_node = next(
                (
                    node
                    for node in state.nodes.values()
                    if node.id == quote_id or f"{task_id}:{node.id}" == quote_id
                ),
                None,
            )
            if quoted_node is not None and quoted_node.status in ACTIVE_NODE_STATUSES:
                queued = state.enqueue(
                    quoted_node.id, text, sender="user", quote_id=str(quote_id)
                )
                await self._emit_room_message(
                    event_queue,
                    state,
                    task_id,
                    context_id,
                    "message.queued",
                    f"已排队，将在 @{quoted_node.agent_name} 当前工作结束后投递",
                    queued_message_id=queued.id,
                    node_id=quoted_node.id,
                )
                return
            if quoted_node is not None and quoted_node.status == "completed":
                await self._spawn_followup_node(
                    state, text, quoted_node, task_id, context_id, event_queue
                )
                return

        if interrupt and active:
            node = active[0]
            if node.a2a_task_id:
                await self._remote.cancel_task(node.agent_url, node.a2a_task_id)
            node.status = "canceled"
            for blocked in state.blocked_nodes():
                blocked.status = "invalidated"
            await self._emit(
                event_queue, state, task_id, context_id,
                "node.canceled",
                node_id=node.id,
                agent_name=node.agent_name,
            )
            await self._emit_room_message(
                event_queue,
                state,
                task_id,
                context_id,
                "task.interrupted",
                f"已打断 @{node.agent_name} 的当前工作，转入新任务",
            )
            await self._spawn_followup_node(
                state, text, node, task_id, context_id, event_queue, deps=[]
            )
            return

        target = active[0] if active else next(
            (n for n in state.nodes.values() if n.status in {"pending", "ready"}),
            None,
        )
        if target is not None:
            queued = state.enqueue(
                target.id,
                text,
                sender="user",
                quote_id=str(quote_id) if quote_id else None,
            )
            await self._emit_room_message(
                event_queue,
                state,
                task_id,
                context_id,
                "message.queued",
                f"已排队，将在 @{target.agent_name} 当前工作结束后投递",
                queued_message_id=queued.id,
                node_id=target.id,
            )
            return

        # No plan left to attach to: start a follow-up plan in place.
        await self._plan_and_launch(
            state, text, task_id, context_id, event_queue, follow_up=True
        )

    async def _spawn_followup_node(
        self,
        state: OrchestrationState,
        text: str,
        anchor: NodeState,
        task_id: str,
        context_id: str,
        event_queue: EventQueue,
        *,
        deps: list[str] | None = None,
    ) -> None:
        if state.derived_count >= self._max_derived_nodes:
            return
        state.derived_count += 1
        node_id = f"{anchor.id}-f{state.derived_count}"
        input_text = build_followup_input(anchor.agent_name, anchor.output, text)
        followup = NodeState(
            id=node_id,
            name=f"继续 · {anchor.name}",
            agent_name=anchor.agent_name,
            agent_url=anchor.agent_url,
            deps=list(deps if deps is not None else [anchor.id]),
            input_text=input_text,
            derived=True,
        )
        state.nodes[node_id] = followup
        await self._emit_room_message(
            event_queue,
            state,
            task_id,
            context_id,
            "followup.dispatched",
            f"已创建 @{anchor.agent_name} 的接续任务",
        )
        await self._persist(event_queue, state, task_id, context_id)
        self._start_runner(task_id)

    async def _deliver_queued(
        self,
        state: OrchestrationState,
        node: NodeState,
        task_id: str,
        context_id: str,
        queue: EventQueue,
    ) -> None:
        messages = state.take_queued(node.id)
        if not messages:
            return
        text = "\n\n".join(message.text for message in messages)
        for message in messages:
            await self._emit(
                queue, state, task_id, context_id,
                "message.delivered",
                node_id=node.id,
                message_id=message.id,
            )
        await self._spawn_followup_node(
            state, text, node, task_id, context_id, queue, deps=[node.id]
        )
        await self._emit_room_message(
            queue,
            state,
            task_id,
            context_id,
            "message.delivered",
            f"排队消息已投递给 @{node.agent_name}",
            node_id=node.id,
        )

    async def _arbitrate_mentions(
        self,
        state: OrchestrationState,
        node: NodeState,
        task_id: str,
        context_id: str,
        queue: EventQueue,
    ) -> None:
        if not node.output:
            return
        agents = await self._registry.list()
        known = {agent.name: agent for agent in agents}
        for name in dict.fromkeys(re.findall(r"@([A-Za-z0-9_-]+)", node.output)):
            if name == node.agent_name or name not in known:
                continue
            if state.assist_nodes_for(name, node.id):
                continue
            if state.derived_count >= self._max_derived_nodes:
                return
            state.derived_count += 1
            helper_id = f"{node.id}-a{state.derived_count}"
            helper = NodeState(
                id=helper_id,
                name=f"协助 · {node.name}",
                agent_name=name,
                agent_url=known[name].card_url,
                deps=[],
                input_text=build_assist_input(node.agent_name, node.output),
                derived=True,
                assist_requested_by=node.id,
                source_message_id=node.id,
            )
            state.nodes[helper_id] = helper
            await self._join_members(
                state, [name], "agent_mention", task_id, context_id, queue
            )
            await self._emit_room_message(
                queue,
                state,
                task_id,
                context_id,
                "mention.arbitrated",
                f"@{node.agent_name} 请求 @{name} 协助，已加入工作",
            )
            await self._persist(queue, state, task_id, context_id)
            self._start_runner(task_id)

    # ---------------------------------------------------------------- retry

    async def _replan(
        self,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
        queue: EventQueue,
    ) -> bool:
        nodes = state.nodes.values()
        try:
            draft = await self._planner.plan(
                state.rationale or "继续完成任务",
                reason=build_replan_reason(nodes),
                context=build_replan_context(nodes) or None,
            )
        except PlanningFailed:
            return False
        agents = await self._registry.list()
        agent_urls = {agent.name: agent.card_url for agent in agents}
        state.plan_version += 1
        state.plan_id = self._new_plan_id()
        state.nodes = {}
        state.queue = {}
        for node_draft in draft.nodes:
            state.nodes[node_draft.id] = NodeState(
                id=node_draft.id,
                name=node_draft.name,
                agent_name=node_draft.agent_name,
                agent_url=agent_urls.get(node_draft.agent_name, ""),
                deps=list(node_draft.deps),
                input_text=str(node_draft.input.get("text", "")),
            )
        await self._emit(
            queue, state, task_id, context_id,
            "plan.created",
            TaskState.TASK_STATE_WORKING,
            plan_id=state.plan_id,
            plan_version=state.plan_version,
            rationale=draft.rationale,
            nodes=[
                {
                    "id": n.id,
                    "name": n.name,
                    "agent_name": n.agent_name,
                    "deps": n.deps,
                }
                for n in state.nodes.values()
            ],
        )
        await self._join_members(
            state,
            [n.agent_name for n in state.nodes.values() if n.agent_name],
            "plan",
            task_id,
            context_id,
            queue,
        )
        return True

    # ------------------------------------------------------------ emitting

    async def _emit(
        self,
        queue: EventQueue,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
        kind: str,
        state_name: TaskState = TaskState.TASK_STATE_WORKING,
        *,
        message: Message | None = None,
        **metadata: Any,
    ) -> None:
        await queue.enqueue_event(
            _status_update(
                task_id,
                context_id,
                state_name,
                kind=kind,
                orch_state=state,
                message=message,
                **metadata,
            )
        )

    async def _persist(
        self,
        queue: EventQueue,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
    ) -> None:
        await self._emit(queue, state, task_id, context_id, "state.updated")

    async def _emit_room_message(
        self,
        queue: EventQueue,
        state: OrchestrationState,
        task_id: str,
        context_id: str,
        kind: str,
        text: str,
        *,
        node_id: str | None = None,
        queued_message_id: str | None = None,
    ) -> None:
        message = new_text_message(
            text, role=Role.ROLE_AGENT, task_id=task_id, context_id=context_id
        )
        room: RoomOptions = {"sender": "assistant", "role": "assistant"}
        if node_id:
            room["node_id"] = node_id
        ParseDict({A2A_ROOM_URI: room}, message.metadata)
        ParseDict({"cw_thought": True}, message.parts[0].metadata)
        await self._emit(
            queue,
            state,
            task_id,
            context_id,
            kind,
            message=message,
            node_id=node_id,
            queued_message_id=queued_message_id,
        )

    async def _join_members(
        self,
        state: OrchestrationState,
        names: list[str],
        reason: str,
        task_id: str,
        context_id: str,
        queue: EventQueue,
    ) -> None:
        records = await self._registry.list()
        known = {record.name: record for record in records}
        for name in dict.fromkeys(names):
            record = known.get(name)
            if record is None:
                continue
            if not state.add_member(name, record.card_url, reason):
                continue
            await self._emit(
                queue, state, task_id, context_id,
                "room.participant_joined",
                agent_name=name,
                agent_url=record.card_url,
                reason=reason,
            )
