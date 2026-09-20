from __future__ import annotations

import asyncio
import logging
import re
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from a2a.helpers import new_task
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks.task_store import TaskStore
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from google.protobuf import struct_pb2, timestamp_pb2
from google.protobuf.json_format import ParseDict
from pydantic import BaseModel, create_model

from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.markers import parse_marker
from choirworks.a2a.patch import PatchResult, PlanPatch, apply_patch
from choirworks.a2a.registry import AgentRegistry
from choirworks.a2a.room import RoomOptions, room_options
from choirworks.a2a.state import (
    ACTIVE_NODE_STATUSES,
    STATE_JSON_KEY,
    NodeState,
    OrchestrationState,
)
from choirworks.core.context import (
    ContextBriefBuilder,
    build_assist_input,
    build_assistance_decision_user,
    build_continuation_text,
    build_dispatch_text,
    build_outcome_user,
    build_repair_user,
)
from choirworks.core.llm import LiteLLMClient
from choirworks.core.planner import PlanDraft, Planner, PlanningFailed
from choirworks.store.contexts import ContextStore
from choirworks.tools import (
    AskUserFunction,
    CallSubagentFunction,
    CreatePlanFunction,
    FunctionContext,
    FunctionRegistry,
    RevisePlanFunction,
    emit_function_call,
    emit_function_error,
)

logger = logging.getLogger(__name__)


class OutcomeDecision(BaseModel):
    """What an agent's final reply means for the plan."""

    intent: Literal["deliver", "need_info", "revise"]
    question: str = ""
    target_agent: str | None = None
    instruction: str = ""
    patch: PlanPatch | None = None
    reasoning: str = ""


def outcome_decision_schema(
    candidate_names: Sequence[str],
) -> type[OutcomeDecision]:
    fields: dict[str, Any] = {}
    if candidate_names:
        fields["target_agent"] = (Literal[*candidate_names] | None, None)
    return create_model("OutcomeDecision", __base__=OutcomeDecision, **fields)


OUTCOME_SYSTEM = """You are the orchestrator of a multi-agent group.
Read an agent's final reply and decide what it means for the plan:
- intent="deliver": the reply is the finished work (default when unsure)
- intent="need_info": the reply asks for information, help from a member, or a human decision
- intent="revise": the reply suggests the plan should change (add or remove work)

When intent="need_info", put what is needed into question and set target_agent to the
listed candidate who can help; leave target_agent empty when a human must answer.
When intent="deliver" or intent="revise", leave question, target_agent and instruction empty.
Return only JSON matching the schema."""

ASSISTANCE_SYSTEM = """You are the orchestrator of a multi-agent group.
An agent is blocked and needs help. Decide how to handle it:
- set target_agent to another registered agent that can help
- leave target_agent empty to escalate to a human

Set intent="need_info". When target_agent is set, instruction should describe the
task. Return only JSON matching the schema.
- reasoning: one short sentence explaining your decision."""

REPAIR_SYSTEM = """You are the orchestrator of a multi-agent group.
Some tasks in the plan failed after retries. Produce an incremental repair patch:
- intent="revise" with a patch that adds replacement tasks and/or invalidates tasks
- added tasks may only depend on existing task ids
- do not repeat work that is already completed; keep the plan minimal
Return only JSON matching the schema."""


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
    **metadata: Any,
) -> TaskStatusUpdateEvent:
    meta: dict[str, Any] = {}
    if kind:
        meta["kind"] = kind
    meta.update(metadata)
    timestamp = timestamp_pb2.Timestamp()
    timestamp.GetCurrentTime()
    return TaskStatusUpdateEvent(
        task_id=task_id,
        context_id=context_id,
        status=TaskStatus(state=state, timestamp=timestamp),
        metadata=_struct(meta) if meta else None,
    )


@dataclass
class SessionRuntime:
    """In-memory state for one conversation (A2A context).

    The session owns the orchestration state; the task is only the event
    channel for the current turn. Events must use ``task_id``/``queue`` of
    the active request because the SDK validates both.
    """

    context_id: str
    state: OrchestrationState
    lock: asyncio.Lock
    task_id: str
    queue: EventQueue
    runner: asyncio.Task | None = None
    node_tasks: dict[asyncio.Task, NodeState] = field(default_factory=dict)


def _join_text(parts: Any) -> str:
    return "\n".join(p.text for p in parts if p.HasField("text"))


_AFFIRMATIVE_ANSWERS = {"确认", "确定", "打断", "是", "yes", "y", "ok"}


def _is_affirmative(text: str) -> bool:
    return text.strip().lower() in _AFFIRMATIVE_ANSWERS


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

        self._sessions: dict[str, SessionRuntime] = {}
        self._session_gate = asyncio.Lock()
        self._context_store: ContextStore | None = None

        self._functions = FunctionRegistry()
        self._functions.register(CreatePlanFunction())
        self._functions.register(RevisePlanFunction())
        self._functions.register(AskUserFunction())
        self._functions.register(CallSubagentFunction())

    # ------------------------------------------------------------- lifecycle

    def set_task_store(self, task_store: TaskStore) -> None:
        """Injected by the app so plans can read and reload session history."""
        self._brief_builder.set_task_store(task_store)

    def set_context_store(self, context_store: ContextStore) -> None:
        """Injected by the app as the canonical conversation state store."""
        self._context_store = context_store

    async def _load_session_state(self, context_id: str) -> OrchestrationState | None:
        """Load conversation state from the canonical contexts row."""
        if self._context_store is None:
            return None
        record = await self._context_store.get(context_id)
        if record is None:
            return None
        try:
            return OrchestrationState.from_json(record.state)
        except (ValueError, TypeError):
            logger.warning("Invalid context state for %s", context_id)
            return None

    async def _ensure_session(
        self, context: RequestContext, event_queue: EventQueue
    ) -> SessionRuntime:
        context_id = context.context_id or ""
        task_id = context.task_id or ""
        loaded = False
        async with self._session_gate:
            runtime = self._sessions.get(context_id)
            if runtime is None:
                state = await self._load_session_state(context_id)
                runtime = SessionRuntime(
                    context_id=context_id,
                    state=state or OrchestrationState(),
                    lock=asyncio.Lock(),
                    task_id=task_id,
                    queue=event_queue,
                )
                self._sessions[context_id] = runtime
                loaded = True
        runtime.task_id = task_id
        runtime.queue = event_queue
        if loaded:
            for intervention in runtime.state.normalize_cancel_requests():
                await self._emit_event(
                    runtime,
                    "intervention.expired",
                    intervention_id=intervention.id,
                    node_id=intervention.target_node_id or "",
                )
        return runtime

    def _evict_session(self, context_id: str) -> None:
        runtime = self._sessions.pop(context_id, None)
        if runtime is not None:
            runtime.runner = None
            runtime.node_tasks.clear()

    def session_is_active(self, context_id: str) -> bool:
        runtime = self._sessions.get(context_id)
        return (
            runtime is not None
            and runtime.runner is not None
            and not runtime.runner.done()
        )

    def drop_session(self, context_id: str) -> None:
        self._evict_session(context_id)

    async def execute(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        """Route one inbound message; background runners do the actual work."""
        text = (context.get_user_input() or "").strip()
        task_id = context.task_id or ""
        context_id = context.context_id or ""

        if _is_resume_message(context.message):
            await self._resume_task(context, event_queue)
            return

        runtime = await self._ensure_session(context, event_queue)

        if context.current_task is None:
            initial_task = new_task(
                task_id=task_id,
                context_id=context_id,
                state=TaskState.TASK_STATE_SUBMITTED,
                history=[context.message] if context.message else None,
            )
            await event_queue.enqueue_event(initial_task)

        updater = TaskUpdater(event_queue, task_id, context_id)
        room = room_options(context.message)
        mentions = list(room.get("mentions") or [])
        for name in re.findall(r"@([A-Za-z0-9_-]+)", text):
            if name not in mentions:
                mentions.append(name)
        if mentions:
            room["mentions"] = mentions

        async with runtime.lock:
            state = runtime.state
            if state.pending_interventions():
                if text:
                    await self._answer_intervention(runtime, text)
                return

            if runtime.runner is not None and not runtime.runner.done():
                await self._route_message(runtime, text, room)
                return

            if state.has_pending_work():
                await self._route_message(runtime, text, room)
                self._start_runner(runtime)
                return

            if not text:
                await updater.complete()
                self._evict_session(context_id)
                return

            await updater.start_work()
            await self._plan_and_launch(runtime, text, room=room)

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        task_id = context.task_id or ""
        context_id = context.context_id or ""
        # Emit the terminal event first: the SDK cancels the producer before
        # calling us and closes the agent queue as soon as it unwinds.
        await event_queue.enqueue_event(
            _status_update(
                task_id,
                context_id,
                TaskState.TASK_STATE_CANCELED,
                kind="task.canceled",
            )
        )
        runtime = self._sessions.get(context_id)
        if runtime is None:
            return
        async with runtime.lock:
            runner = runtime.runner
            runtime.runner = None
            if runner is not None:
                runner.cancel()
            state = runtime.state
            for node in list(state.nodes.values()):
                if node.status in ACTIVE_NODE_STATUSES | {"ready"}:
                    node.status = "canceled"
            await self._persist(runtime)
            for node in list(state.nodes.values()):
                if node.status == "canceled" and node.a2a_task_id:
                    await self._remote.cancel_task(node.agent_url, node.a2a_task_id)
            self._evict_session(context_id)

    async def shutdown(self) -> None:
        runtimes = list(self._sessions.values())
        for runtime in runtimes:
            if runtime.runner is not None:
                runtime.runner.cancel()
        for runtime in runtimes:
            if runtime.runner is not None:
                try:
                    await runtime.runner
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
            for node_task in list(runtime.node_tasks):
                node_task.cancel()
        self._sessions.clear()

    async def _resume_task(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        runtime = await self._ensure_session(context, event_queue)
        state = await self._load_session_state(runtime.context_id)
        if state is None:
            await self._emit_event(
                runtime,
                "task.failed", TaskState.TASK_STATE_FAILED,
                reason="resume_without_state",
            )
            self._evict_session(runtime.context_id)
            return
        async with runtime.lock:
            runtime.state = state
        await self._resume(runtime)

    async def _resume(self, runtime: SessionRuntime) -> None:
        """Re-attach to remote work after a process restart."""
        for intervention in runtime.state.normalize_cancel_requests():
            await self._emit_event(
                runtime,
                "intervention.expired",
                intervention_id=intervention.id,
                node_id=intervention.target_node_id or "",
            )
        for node in runtime.state.nodes.values():
            if node.status in ACTIVE_NODE_STATUSES:
                node.status = "resume" if node.a2a_task_id else "pending"
        await self._persist(runtime)
        self._start_runner(runtime)

    # ---------------------------------------------------------------- plan

    def _new_plan_id(self) -> str:
        return f"plan-{uuid.uuid4().hex[:8]}"

    async def _emit_thought_chunk(
        self,
        runtime: SessionRuntime,
        *,
        text: str,
        author: str,
        append: bool,
        last_chunk: bool,
    ) -> None:
        part = Part(text=text)
        ParseDict({"cw_thought": True}, part.metadata)
        await runtime.queue.enqueue_event(
            TaskArtifactUpdateEvent(
                task_id=runtime.task_id,
                context_id=runtime.context_id,
                artifact=Artifact(
                    artifact_id=f"{author}:thinking",
                    parts=[part],
                    metadata=_struct({"author": author}),
                ),
                append=append,
                last_chunk=last_chunk,
            )
        )

    async def _stream_plan(
        self,
        runtime: SessionRuntime,
        request: str,
        *,
        reason: str | None = None,
        context: str | None = None,
    ) -> PlanDraft:
        """Stream the planner's thinking to subscribers, then return the plan."""
        draft: PlanDraft | None = None
        thinking_parts: list[str] = []
        first_chunk = True
        async for item in self._planner.plan(
            request, reason=reason, context=context
        ):
            if isinstance(item, PlanDraft):
                draft = item
                continue
            thinking_parts.append(item)
            await self._emit_thought_chunk(
                runtime,
                text=item,
                author="assistant",
                append=not first_chunk,
                last_chunk=False,
            )
            first_chunk = False
        thinking = "".join(thinking_parts)
        if thinking:
            await self._emit_thought_chunk(
                runtime,
                text=thinking,
                author="assistant",
                append=False,
                last_chunk=True,
            )
        if draft is None:
            raise PlanningFailed("planner stream ended without a plan")
        return draft

    async def _plan_and_launch(
        self,
        runtime: SessionRuntime,
        text: str,
        *,
        room: RoomOptions | None = None,
    ) -> None:
        state = runtime.state
        state.start_new_plan(self._new_plan_id())
        context_brief = await self._brief_builder.build(
            runtime.context_id, exclude_task_id=runtime.task_id
        )
        create_plan = self._functions.get("create_plan")
        assert create_plan is not None
        ctx = FunctionContext(executor=self, runtime=runtime)
        try:
            draft = await self._stream_plan(
                runtime, text, context=context_brief or None
            )
        except PlanningFailed as exc:
            logger.warning("Planning failed for task %s: %s", runtime.task_id, exc)
            await self._persist(runtime)
            await emit_function_error(
                self, runtime, create_plan, str(exc),
                state_name=TaskState.TASK_STATE_FAILED,
            )
            self._evict_session(runtime.context_id)
            return

        result = await create_plan.execute(ctx, draft)
        await emit_function_call(
            self, runtime, create_plan, draft, result,
            state_name=TaskState.TASK_STATE_WORKING,
        )

        agents = await self._registry.list()
        agent_urls = {agent.name: agent.card_url for agent in agents}
        mention_targets = [
            name
            for name in (room or {}).get("mentions", [])
            if name in agent_urls
        ]
        if mention_targets:
            await self._join_members(runtime, mention_targets, "human_mention")
        await self._persist(runtime)
        self._start_runner(runtime)

    # --------------------------------------------------------------- runner

    def _start_runner(self, runtime: SessionRuntime) -> None:
        if runtime.runner is not None and not runtime.runner.done():
            return
        runtime.runner = asyncio.create_task(
            self._run_plan(runtime), name=f"choirworks-runner:{runtime.context_id}"
        )

    async def _run_plan(self, runtime: SessionRuntime) -> None:
        context_id = runtime.context_id
        task_id = runtime.task_id
        try:
            while True:
                async with runtime.lock:
                    state = runtime.state
                    for node in list(state.failed_nodes()):
                        if node.attempt < self._max_node_attempts:
                            node.status = "pending"
                            node.error = None
                            await self._emit_event(
                                runtime,
                                "node.retry_scheduled",
                                node_id=node.id,
                                attempt=node.attempt,
                            )

                    ready = state.ready_nodes()
                    slots = max(0, self._max_parallel - self._pending_count(runtime))
                    dispatched = 0
                    for node in ready[:slots]:
                        mode = (
                            "resume"
                            if node.status == "resume"
                            else ("continue" if node.status == "ready" else "dispatch")
                        )
                        if mode != "resume":
                            node.status = "dispatched"
                        node_task = asyncio.create_task(
                            self._execute_node(runtime, node, mode=mode),
                            name=f"choirworks-node:{context_id}:{node.id}",
                        )
                        runtime.node_tasks[node_task] = node
                        dispatched += 1
                    if dispatched:
                        await self._persist(runtime)

                pending = dict(runtime.node_tasks)
                if pending:
                    done, _ = await asyncio.wait(
                        set(pending), return_when=asyncio.FIRST_COMPLETED
                    )
                    for finished in done:
                        node = runtime.node_tasks.pop(finished)
                        exception = finished.exception()
                        if exception is not None:
                            node.status = "failed"
                            node.error = str(exception)
                            await self._emit_event(
                                runtime,
                                "node.failed", node_id=node.id, error=node.error,
                            )
                    if self._retry_backoff > 0 and any(
                        n.status == "failed" and n.attempt < self._max_node_attempts
                        for n in runtime.state.nodes.values()
                    ):
                        await asyncio.sleep(self._retry_backoff)
                    continue

                async with runtime.lock:
                    state = runtime.state
                    if any(
                        n.status == "failed" and n.attempt < self._max_node_attempts
                        for n in state.nodes.values()
                    ):
                        continue
                    if state.input_required_nodes():
                        progress = await self._settle_input(runtime)
                        if progress:
                            continue
                        await self._persist(runtime)
                        await self._emit_event(
                            runtime,
                            "task.requires_input",
                            TaskState.TASK_STATE_INPUT_REQUIRED,
                        )
                        return
                    if state.all_completed():
                        await self._emit_event(
                            runtime,
                            "task.completed", TaskState.TASK_STATE_COMPLETED,
                        )
                        self._evict_session(context_id)
                        return
                    if state.has_failures():
                        recovered = False
                        if self._replan_on_failure:
                            recovered = await self._repair_plan(runtime)
                        if recovered:
                            continue
                        await self._emit_event(
                            runtime,
                            "task.failed", TaskState.TASK_STATE_FAILED,
                            reason="nodes_failed",
                            nodes=[
                                {
                                    "id": n.id,
                                    "agent_name": n.agent_name,
                                    "error": n.error,
                                }
                                for n in state.failed_nodes()
                            ],
                        )
                        self._evict_session(context_id)
                        return
                    if state.has_pending_work():
                        schedulable = bool(state.ready_nodes()) or (
                            self._pending_count(runtime) > 0
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
                    await self._emit_event(
                        runtime,
                        "task.failed", TaskState.TASK_STATE_FAILED,
                        reason="stalled",
                    )
                    self._evict_session(context_id)
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - never let the runner die silently
            logger.exception("Runner failed for task %s", task_id)
            if context_id in self._sessions:
                try:
                    await self._persist(runtime)
                    await self._emit_event(
                        runtime,
                        "task.failed", TaskState.TASK_STATE_FAILED,
                        reason="runner_error",
                        error=str(exc),
                    )
                except Exception:  # noqa: BLE001 - queue may already be closed
                    logger.exception("Failed to emit task failure for %s", task_id)
                self._evict_session(context_id)
        finally:
            runtime.runner = None
            for node_task in list(runtime.node_tasks):
                node_task.cancel()
            runtime.node_tasks.clear()

    def _pending_count(self, runtime: SessionRuntime) -> int:
        return len(runtime.node_tasks)

    async def _build_node_text(
        self, runtime: SessionRuntime, node: NodeState
    ) -> str:
        """Assemble the dispatch text, including any resolved Q&A round."""
        agents = await self._registry.list()
        by_name = {agent.name: agent for agent in agents}
        if node.answer_text is not None:
            text = build_continuation_text(
                node,
                runtime.state,
                by_name,
                question=node.question or "",
                answer=node.answer_text,
            )
            node.answer_text = None
            node.question = None
            return text
        return build_dispatch_text(node, runtime.state, by_name)

    async def _execute_node(
        self,
        runtime: SessionRuntime,
        node: NodeState,
        *,
        mode: str = "dispatch",
    ) -> None:
        continuation = mode == "continue"
        if mode != "resume":
            node.attempt += 1
        if mode == "dispatch":
            await self._emit_event(
                runtime,
                "node.dispatch_intent",
                node_id=node.id,
                attempt=node.attempt,
            )
        elif mode == "resume":
            await self._emit_event(
                runtime,
                "node.resumed",
                node_id=node.id,
                a2a_task_id=node.a2a_task_id,
            )
        current = "working"
        try:
            async with asyncio.timeout(self._node_timeout):
                if mode == "resume":
                    current = await self._resume_remote(runtime, node)
                else:
                    text = await self._build_node_text(runtime, node)
                    current = await self._stream_remote(
                        runtime, node, text, continuation=continuation
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
            decision = await self._interpret_outcome(runtime, node)
            if decision.intent == "need_info":
                node.status = "input_required"
                node.question = decision.question or node.output
                # The remote reply was a normal completion, so the remote
                # task is terminal; answering must re-dispatch a fresh task.
                node.a2a_task_id = None
                await self._emit_event(
                    runtime,
                    "node.input_required",
                    TaskState.TASK_STATE_INPUT_REQUIRED,
                    node_id=node.id,
                    agent_name=node.agent_name,
                    question=node.question or "",
                )
            else:
                if decision.intent == "revise" and decision.patch is not None:
                    async with runtime.lock:
                        await self._revise_plan(runtime, decision.patch)
                node.status = "completed"
                await self._emit_event(
                    runtime,
                    "node.completed",
                    node_id=node.id,
                    agent_name=node.agent_name,
                    output_summary=(node.output or "")[:200],
                )
                await self._arbitrate_mentions(runtime, node)
                await self._deliver_queued(runtime, node)
        elif current == "canceled":
            node.status = "canceled"
        elif current == "input_required":
            node.status = "input_required"
            await self._emit_event(
                runtime,
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
            await self._emit_event(
                runtime,
                "node.failed",
                node_id=node.id,
                error=node.error or "unknown error",
            )

        for intervention in runtime.state.expire_cancel_requests(node.id):
            await self._emit_event(
                runtime,
                "intervention.expired",
                intervention_id=intervention.id,
                node_id=node.id,
            )
        await self._persist(runtime)

    async def _stream_remote(
        self,
        runtime: SessionRuntime,
        node: NodeState,
        text: str,
        *,
        continuation: bool,
    ) -> str:
        remote_task_id = node.a2a_task_id if continuation else None
        chunks = self._remote.send_text(
            node.agent_url,
            text,
            task_id=remote_task_id,
            context_id=runtime.context_id,
            message_id=f"{runtime.context_id}:{node.id}:{node.attempt}",
        )
        current = await self._consume_chunks(runtime, node, chunks)
        return await self._ensure_terminal(runtime, node, current)

    async def _resume_remote(self, runtime: SessionRuntime, node: NodeState) -> str:
        if not node.a2a_task_id:
            return "failed"
        current = "working"
        try:
            chunks = self._remote.subscribe_task(node.agent_url, node.a2a_task_id)
            current = await self._consume_chunks(runtime, node, chunks)
        except Exception as exc:  # noqa: BLE001 - task may already be terminal
            logger.debug("Resume subscribe failed for %s: %s", node.id, exc)
        return await self._ensure_terminal(runtime, node, current)

    async def _ensure_terminal(
        self,
        runtime: SessionRuntime,
        node: NodeState,
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
                current = await self._consume_chunks(runtime, node, chunks)
            except Exception as exc:  # noqa: BLE001 - retry via snapshot
                logger.debug("Follow subscribe failed for %s: %s", node.id, exc)
                await asyncio.sleep(0.2)
        return current

    async def _consume_chunks(
        self,
        runtime: SessionRuntime,
        node: NodeState,
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
                await self._emit_event(
                    runtime,
                    "node.dispatched",
                    node_id=node.id,
                    a2a_task_id=node.a2a_task_id,
                )
            elif chunk.HasField("status_update"):
                remote_state = chunk.status_update.status.state
                mapped = _REMOTE_STATE_MAP.get(remote_state)
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
                await runtime.queue.enqueue_event(
                    TaskArtifactUpdateEvent(
                        task_id=runtime.task_id,
                        context_id=runtime.context_id,
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
                await runtime.queue.enqueue_event(
                    TaskArtifactUpdateEvent(
                        task_id=runtime.task_id,
                        context_id=runtime.context_id,
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

    async def _settle_input(self, runtime: SessionRuntime) -> bool:
        state = runtime.state
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
                    intervention = state.add_intervention(node.id, node.question or "")
                intervention.status = "resolved"
                intervention.answer = helper.output
                intervention.responder = helper.id
                node.answer_text = helper.output
                node.status = "ready"
                await self._emit_event(
                    runtime,
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
            if decision is not None and decision.target_agent:
                if await self._spawn_assist(runtime, node, decision):
                    progress = True
                else:
                    await self._request_human(runtime, node)
            else:
                await self._request_human(runtime, node)
        return progress

    async def _interpret_outcome(
        self, runtime: SessionRuntime, node: NodeState
    ) -> OutcomeDecision:
        """Decide what a completed node's final reply means for the plan."""
        marker = parse_marker(node.output)
        if marker is not None:
            return OutcomeDecision(intent=marker.intent, question=marker.text)
        if not node.output:
            return OutcomeDecision(intent="deliver")
        agents = await self._registry.list()
        candidates = [agent for agent in agents if agent.name != node.agent_name]
        user = build_outcome_user(
            node.agent_name,
            node.input_text,
            node.output,
            candidates,
        )
        schema = outcome_decision_schema([agent.name for agent in candidates])
        try:
            async for item in self._llm.stream_structured(
                system=OUTCOME_SYSTEM,
                user=user,
                schema=schema,
                tool_name="OutcomeDecision",
            ):
                if isinstance(item, OutcomeDecision):
                    return item
        except Exception:  # noqa: BLE001 - default to delivering the output
            logger.exception("outcome interpretation failed for %s", node.id)
        return OutcomeDecision(intent="deliver")

    async def _request_human(self, runtime: SessionRuntime, node: NodeState) -> None:
        from choirworks.tools.ask_user import AskUserArgs

        func = self._functions.get("ask_user")
        assert func is not None
        args = AskUserArgs(node_id=node.id, question=node.question or node.output or "")
        ctx = FunctionContext(executor=self, runtime=runtime)
        result = await func.execute(ctx, args)
        await emit_function_call(
            self, runtime, func, args, result,
            state_name=TaskState.TASK_STATE_INPUT_REQUIRED,
        )

    async def _decide_assistance(
        self, node: NodeState
    ) -> OutcomeDecision | None:
        if node.question is None:
            return None
        agents = await self._registry.list()
        candidates = [agent for agent in agents if agent.name != node.agent_name]
        if not candidates:
            return OutcomeDecision(intent="need_info")
        user = build_assistance_decision_user(
            node.agent_name,
            node.question or node.input_text,
            candidates,
        )
        schema = outcome_decision_schema([agent.name for agent in candidates])
        try:
            async for item in self._llm.stream_structured(
                system=ASSISTANCE_SYSTEM,
                user=user,
                schema=schema,
                tool_name="OutcomeDecision",
            ):
                if isinstance(item, OutcomeDecision):
                    return item
        except Exception:  # noqa: BLE001 - fall back to human
            logger.exception("assistance decision failed for %s", node.id)
            return OutcomeDecision(intent="need_info")
        return OutcomeDecision(intent="need_info")

    async def _spawn_assist(
        self,
        runtime: SessionRuntime,
        node: NodeState,
        decision: OutcomeDecision,
    ) -> bool:
        from choirworks.tools.call_subagent import CallSubagentArgs

        func = self._functions.get("call_subagent")
        assert func is not None
        args = CallSubagentArgs(
            requester_node_id=node.id,
            target_agent=decision.target_agent or "",
            instruction=decision.instruction,
        )
        ctx = FunctionContext(executor=self, runtime=runtime)
        result = await func.execute(ctx, args)
        if not result.success:
            return False
        await emit_function_call(self, runtime, func, args, result)
        return True

    async def _answer_intervention(
        self,
        runtime: SessionRuntime,
        text: str,
    ) -> None:
        state = runtime.state
        pending = state.pending_interventions()
        if not pending or not text:
            return
        intervention = pending[0]
        intervention.status = "resolved"
        intervention.answer = text
        intervention.responder = "human"
        if intervention.kind == "confirm_cancel":
            target = state.nodes.get(intervention.target_node_id or "")
            if target is not None and _is_affirmative(text):
                await self._cancel_node(runtime, target)
            await self._emit_event(
                runtime,
                "intervention.resolved",
                node_id=intervention.node_id,
                intervention_id=intervention.id,
                responder="human",
            )
            await self._persist(runtime)
            self._start_runner(runtime)
            return
        node = state.nodes.get(intervention.node_id)
        if node is not None:
            node.answer_text = text
            node.status = "ready"
        await self._emit_event(
            runtime,
            "intervention.resolved",
            node_id=intervention.node_id,
            intervention_id=intervention.id,
            responder="human",
        )
        self._start_runner(runtime)

    # -------------------------------------------------------------- routing

    async def _route_message(
        self,
        runtime: SessionRuntime,
        text: str,
        room: RoomOptions,
    ) -> None:
        state = runtime.state
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
                    if node.id == quote_id
                    or f"{runtime.task_id}:{node.id}" == quote_id
                ),
                None,
            )
            if quoted_node is not None and quoted_node.status in ACTIVE_NODE_STATUSES:
                queued = state.enqueue(
                    quoted_node.id, text, sender="user", quote_id=str(quote_id)
                )
                await self._emit_event(
                    runtime,
                    "message.queued",
                    queued_message_id=queued.id,
                    node_id=quoted_node.id,
                    agent_name=quoted_node.agent_name,
                )
                await self._persist(runtime)
                return
            if quoted_node is not None and quoted_node.status == "completed":
                await self._spawn_followup_node(runtime, text, quoted_node)
                return

        if interrupt and active:
            node = active[0]
            if node.a2a_task_id:
                await self._remote.cancel_task(node.agent_url, node.a2a_task_id)
            node.status = "canceled"
            for blocked in state.blocked_nodes():
                blocked.status = "invalidated"
            await self._emit_event(
                runtime,
                "node.canceled",
                node_id=node.id,
                agent_name=node.agent_name,
            )
            await self._spawn_followup_node(runtime, text, node, deps=[])
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
            await self._emit_event(
                runtime,
                "message.queued",
                queued_message_id=queued.id,
                node_id=target.id,
                agent_name=target.agent_name,
            )
            await self._persist(runtime)
            return

        await self._plan_and_launch(runtime, text)

    async def _spawn_followup_node(
        self,
        runtime: SessionRuntime,
        text: str,
        anchor: NodeState,
        *,
        deps: list[str] | None = None,
    ) -> None:
        state = runtime.state
        if state.derived_count >= self._max_derived_nodes:
            return
        state.derived_count += 1
        node_id = f"{anchor.id}-f{state.derived_count}"
        followup = NodeState(
            id=node_id,
            name="",
            agent_name=anchor.agent_name,
            agent_url=anchor.agent_url,
            deps=list(deps if deps is not None else [anchor.id]),
            input_text=text,
            derived=True,
        )
        state.nodes[node_id] = followup
        await self._emit_event(
            runtime,
            "followup.dispatched",
            node_id=node_id,
            anchor_node_id=anchor.id,
            agent_name=anchor.agent_name,
        )
        await self._persist(runtime)
        self._start_runner(runtime)

    async def _deliver_queued(
        self,
        runtime: SessionRuntime,
        node: NodeState,
    ) -> None:
        messages = runtime.state.take_queued(node.id)
        if not messages:
            return
        text = "\n\n".join(message.text for message in messages)
        for message in messages:
            await self._emit_event(
                runtime,
                "message.delivered",
                node_id=node.id,
                message_id=message.id,
            )
        await self._spawn_followup_node(runtime, text, node, deps=[node.id])

    async def _arbitrate_mentions(
        self,
        runtime: SessionRuntime,
        node: NodeState,
    ) -> None:
        state = runtime.state
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
                name="",
                agent_name=name,
                agent_url=known[name].card_url,
                deps=[],
                input_text=build_assist_input(node.agent_name, node.output),
                derived=True,
                assist_requested_by=node.id,
                source_message_id=node.id,
            )
            state.nodes[helper_id] = helper
            await self._join_members(runtime, [name], "agent_mention")
            await self._emit_event(
                runtime,
                "mention.arbitrated",
                node_id=node.id,
                requester=node.agent_name,
                helper=name,
                helper_node_id=helper_id,
            )
            await self._persist(runtime)
            self._start_runner(runtime)

    # ---------------------------------------------------------------- retry

    async def _repair_plan(self, runtime: SessionRuntime) -> bool:
        """Repair a failed plan with an LLM-produced incremental patch.

        Caller holds ``runtime.lock``; the patch is applied in place so
        completed work and their outputs survive the repair.
        """
        state = runtime.state
        failed_ids = [node.id for node in state.failed_nodes()]
        agents = await self._registry.list()
        if not agents:
            return False
        user = build_repair_user(state.nodes.values(), agents)
        decision: OutcomeDecision | None = None
        try:
            async for item in self._llm.stream_structured(
                system=REPAIR_SYSTEM,
                user=user,
                schema=outcome_decision_schema([agent.name for agent in agents]),
                tool_name="OutcomeDecision",
            ):
                if isinstance(item, OutcomeDecision):
                    decision = item
                    break
        except Exception:  # noqa: BLE001 - repair is best effort
            logger.exception("plan repair failed for %s", runtime.context_id)
            return False
        if decision is None or decision.patch is None:
            return False
        patch = decision.patch
        patch.invalidate = list(dict.fromkeys([*patch.invalidate, *failed_ids]))
        result = await self._revise_plan(runtime, patch)
        return bool(result.added or result.invalidated)

    async def _apply_patch_locked(
        self, runtime: SessionRuntime, patch: PlanPatch
    ) -> PatchResult:
        agents = await self._registry.list()
        agent_urls = {agent.name: agent.card_url for agent in agents}
        state = runtime.state
        result = apply_patch(state, patch, agent_urls)
        for rejected in result.rejected:
            logger.warning("Patch rejected for %s: %s", runtime.context_id, rejected)
        for node_id in result.skipped_in_flight:
            node = state.nodes.get(node_id)
            if node is None:
                continue
            question = (
                f"计划修订建议作废进行中的任务 @{node.agent_name}"
                f"（{patch.reason or '无说明'}）。是否打断？"
                "回复「确认」打断，回复其他内容则保留。"
            )
            intervention = state.add_cancel_request(node_id, question)
            if intervention is None:
                continue
            await self._emit_event(
                runtime,
                "intervention.requested",
                node_id=node_id,
                agent_name=node.agent_name,
                intervention_id=intervention.id,
                intervention_kind="confirm_cancel",
                question=intervention.question,
            )
        added_agents = [
            draft.agent_name for draft in patch.add if draft.agent_name in agent_urls
        ]
        await self._join_members(runtime, added_agents, "plan_revision")
        for node_id in result.invalidated:
            await self._emit_event(
                runtime,
                "node.invalidated",
                node_id=node_id,
                reason=patch.reason,
            )
        await self._persist(runtime)
        return result

    async def _revise_plan(
        self, runtime: SessionRuntime, patch: PlanPatch
    ) -> PatchResult:
        """Apply a plan patch and emit a ``revise_plan`` function-call event.

        Wraps :meth:`_apply_patch_locked` so callers get both the state
        mutation (B-class events) and the model-intent function-call event.
        Returns the underlying :class:`PatchResult` for callers that need
        to inspect ``added`` / ``invalidated``.
        """
        from choirworks.tools.revise_plan import RevisePlanArgs

        func = self._functions.get("revise_plan")
        assert func is not None
        args = RevisePlanArgs(patch=patch)
        ctx = FunctionContext(executor=self, runtime=runtime)
        fn_result = await func.execute(ctx, args)
        await emit_function_call(self, runtime, func, args, fn_result)
        data = fn_result.data or {}
        return PatchResult(
            added=list(data.get("added", [])),
            invalidated=list(data.get("invalidated", [])),
            skipped_in_flight=list(data.get("skipped_in_flight", [])),
            rejected=list(data.get("rejected", [])),
        )

    async def _cancel_node(self, runtime: SessionRuntime, node: NodeState) -> None:
        state = runtime.state
        if node.a2a_task_id and node.agent_url:
            await self._remote.cancel_task(node.agent_url, node.a2a_task_id)
        node.status = "canceled"
        node.a2a_task_id = None
        for blocked in state.blocked_nodes():
            blocked.status = "invalidated"
        await self._emit_event(
            runtime, "node.canceled", node_id=node.id, agent_name=node.agent_name
        )
        await self._persist(runtime)

    # ------------------------------------------------------------ emitting

    async def _emit(
        self,
        queue: EventQueue,
        task_id: str,
        context_id: str,
        kind: str,
        state_name: TaskState = TaskState.TASK_STATE_WORKING,
        **metadata: Any,
    ) -> None:
        await queue.enqueue_event(
            _status_update(
                task_id,
                context_id,
                state_name,
                kind=kind,
                **metadata,
            )
        )

    async def _emit_event(
        self,
        runtime: SessionRuntime,
        kind: str,
        state_name: TaskState = TaskState.TASK_STATE_WORKING,
        **metadata: Any,
    ) -> None:
        await self._emit(
            runtime.queue,
            runtime.task_id,
            runtime.context_id,
            kind,
            state_name,
            **metadata,
        )

    async def _persist(self, runtime: SessionRuntime) -> None:
        snapshot = runtime.state.to_json()
        if self._context_store is not None:
            await self._context_store.upsert_state(runtime.context_id, snapshot)
        await self._emit_event(
            runtime, "state.updated",
            **{STATE_JSON_KEY: snapshot},
        )

    async def _join_members(
        self,
        runtime: SessionRuntime,
        names: list[str],
        reason: str,
    ) -> None:
        state = runtime.state
        records = await self._registry.list()
        known = {record.name: record for record in records}
        for name in dict.fromkeys(names):
            record = known.get(name)
            if record is None:
                continue
            if not state.add_member(name, record.card_url, reason):
                continue
            await self._emit_event(
                runtime,
                "room.participant_joined",
                agent_name=name,
                agent_url=record.card_url,
                reason=reason,
            )
