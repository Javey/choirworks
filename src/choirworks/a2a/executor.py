from __future__ import annotations

import asyncio
import logging
import re
import uuid
from typing import Any

from a2a.helpers import new_task
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks.task_store import TaskStore
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types.a2a_pb2 import (
    Message,
    TaskState,
)

from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.context import ExecutorConfig, OrchestrationContext
from choirworks.a2a.events import (
    emit_event,
    emit_function_call,
    emit_function_error,
    emit_state_delta,
    emit_text_chunk,
    emit_thought_chunk,
)
from choirworks.a2a.helpers import join_members, status_update
from choirworks.a2a.intervention import InterventionManager
from choirworks.a2a.node_executor import NodeExecutor
from choirworks.a2a.patch import PlanPatch
from choirworks.a2a.registry import AgentRegistry
from choirworks.a2a.remote_caller import RemoteAgentCaller
from choirworks.a2a.repair import RepairManager
from choirworks.a2a.room import RoomOptions, room_options
from choirworks.a2a.runner import PlanRunner
from choirworks.a2a.session import SessionManager, SessionRuntime
from choirworks.a2a.state import (
    ACTIVE_NODE_STATUSES,
    NodeState,
)
from choirworks.core.context import ContextBriefBuilder
from choirworks.core.llm import LiteLLMClient
from choirworks.core.planner import PlanDraft, Planner, PlanningFailed
from choirworks.store.contexts import ContextStore
from choirworks.subagents import (
    AssistanceSubagent,
    OutcomeSubagent,
    PlannerSubagent,
    RepairSubagent,
)
from choirworks.tools import (
    FunctionContext,
    ToolCallResult,
    ask_user_func,
    create_plan_func,
    revise_plan_func,
)

logger = logging.getLogger(__name__)


def _is_resume_message(message: Message | None) -> bool:
    if message is None or not message.metadata.fields:
        return False
    return "choirworks.resume" in message.metadata.fields


class ChoirWorksAgentExecutor(AgentExecutor):
    """ChoirWorks orchestration engine as an A2A AgentExecutor.

    The A2A Task is the aggregate root. Plan/node/member/intervention state is
    persisted as A2A event metadata merged into the Task snapshot by the SDK
    ``TaskManager`` + ``DatabaseTaskStore``.

    ``execute()`` routes each inbound message and returns quickly; node work
    runs in background runners, so multiple agents can work and chat at once.

    This class is now a thin A2A protocol layer that delegates to extracted
    components (SessionManager, PlanRunner, NodeExecutor,
    subagents, etc.).  Proxy methods retain backward compatibility with
    tools that access ``ctx.executor._xxx``.
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
        max_revisions: int = 3,
        replan_on_failure: bool = True,
        compaction_threshold: float = 0.8,
        compaction_retention: int = 10,
    ):
        self._registry = registry
        self._remote = remote
        self._planner = planner
        self._llm = llm
        self._config = ExecutorConfig(
            max_parallel=max_parallel,
            node_timeout=node_timeout,
            max_node_attempts=max_node_attempts,
            retry_backoff=retry_backoff,
            max_derived_nodes=max_derived_nodes,
            max_revisions=max_revisions,
            replan_on_failure=replan_on_failure,
        )

        self._brief_builder = ContextBriefBuilder(
            llm,
            compaction_threshold=compaction_threshold,
            compaction_retention=compaction_retention,
        )

        # --- Components ---
        self._session_mgr = SessionManager()
        self._remote_caller = RemoteAgentCaller(remote)

        # Subagents
        self._planner_subagent = PlannerSubagent(llm)
        self._outcome_subagent = OutcomeSubagent(llm)
        self._assistance_subagent = AssistanceSubagent(llm)
        self._repair_subagent = RepairSubagent(llm)

        self._repair_mgr = RepairManager(
            self._session_mgr,
            self._repair_subagent,
            revise_plan_func,
        )
        self._intervention_mgr = InterventionManager(
            self._session_mgr,
            self._assistance_subagent,
            ask_user_func,
            self._remote_caller,
            self._config,
        )
        self._node_executor = NodeExecutor(
            self._session_mgr,
            self._remote_caller,
            self._outcome_subagent,
            self._repair_mgr,
            self._config,
        )
        self._runner = PlanRunner(
            self._session_mgr,
            self._node_executor,
            self._intervention_mgr,
            self._repair_mgr,
            self._config,
        )

    # ------------------------------------------------------------- lifecycle

    def set_task_store(self, task_store: TaskStore) -> None:
        self._brief_builder.set_task_store(task_store)

    def set_context_store(self, context_store: ContextStore) -> None:
        self._session_mgr.set_context_store(context_store)

    @property
    def _sessions(self) -> dict[str, SessionRuntime]:
        return self._session_mgr.sessions

    @property
    def _max_derived_nodes(self) -> int:
        return self._config.max_derived_nodes

    def session_is_active(self, context_id: str) -> bool:
        return self._session_mgr.session_is_active(context_id)

    def drop_session(self, context_id: str) -> None:
        self._session_mgr.drop_session(context_id)

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

        runtime = await self._session_mgr.ensure_session(
            context_id, task_id, event_queue
        )

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
                    orch_ctx = self._build_ctx(runtime)
                    await self._intervention_mgr.answer_intervention(
                        orch_ctx, text
                    )
                    if getattr(runtime, "runner_start_requested", False):
                        runtime.runner_start_requested = False
                        self._start_runner(runtime)
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
                self._session_mgr.evict_session(context_id)
                return

            await updater.start_work()
            await self._plan_and_launch(runtime, text, room=room)

    async def cancel(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        task_id = context.task_id or ""
        context_id = context.context_id or ""
        await event_queue.enqueue_event(
            status_update(task_id, context_id, TaskState.TASK_STATE_CANCELED)
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
        self._session_mgr.evict_session(context_id)

    async def shutdown(self) -> None:
        runtimes = list(self._sessions.values())
        for runtime in runtimes:
            if runtime.runner is not None:
                runtime.runner.cancel()
        for runtime in runtimes:
            if runtime.runner is not None:
                try:
                    await runtime.runner
                except (asyncio.CancelledError, Exception):
                    pass
            for node_task in list(runtime.node_tasks):
                node_task.cancel()
        self._sessions.clear()

    async def _resume_task(
        self, context: RequestContext, event_queue: EventQueue
    ) -> None:
        runtime = await self._session_mgr.ensure_session(
            context.context_id or "", context.task_id or "", event_queue
        )
        state = await self._session_mgr.load_state(runtime.context_id)
        if state is None:
            ctx = self._build_ctx(runtime)
            await emit_event(ctx, "", TaskState.TASK_STATE_FAILED)
            self._session_mgr.evict_session(runtime.context_id)
            return
        async with runtime.lock:
            runtime.state = state
        await self._resume(runtime)

    async def _resume(self, runtime: SessionRuntime) -> None:
        expired = runtime.state.normalize_cancel_requests()
        if expired:
            ctx = self._build_ctx(runtime)
            await emit_state_delta(ctx, interventions={
                iv.id: {
                    "status": "expired",
                    "node_id": iv.target_node_id or "",
                    "kind": "confirm_cancel",
                }
                for iv in expired
            })
        for node in runtime.state.nodes.values():
            if node.status in ACTIVE_NODE_STATUSES:
                node.status = "resume" if node.a2a_task_id else "pending"
        await self._persist(runtime)
        self._start_runner(runtime)

    # ---------------------------------------------------------------- plan

    def _build_ctx(self, runtime: SessionRuntime) -> OrchestrationContext:
        """Build an OrchestrationContext for the given runtime."""
        runtime.runner_start_requested = False
        return OrchestrationContext(
            runtime=runtime,
            registry=self._registry,
            llm=self._llm,
            config=self._config,
            session_mgr=self._session_mgr,
            executor=self,
        )

    def _start_runner(self, runtime: SessionRuntime) -> None:
        ctx = self._build_ctx(runtime)
        self._runner.start_runner(ctx)

    async def _stream_plan(
        self,
        runtime: SessionRuntime,
        request: str,
        ctx: FunctionContext,
        *,
        reason: str | None = None,
        context: str | None = None,
    ) -> ToolCallResult:
        tool_call: ToolCallResult | None = None
        reasoning_parts: list[str] = []
        content_parts: list[str] = []
        first_reasoning = True
        first_content = True
        thought_id = uuid.uuid4().hex
        text_id = uuid.uuid4().hex
        orch_ctx = self._build_ctx(runtime)
        async for item in self._planner.plan(
            request, ctx=ctx, reason=reason, context=context
        ):
            if isinstance(item, ToolCallResult):
                tool_call = item
                continue
            reasoning = getattr(item, "reasoning_content", None)
            content = getattr(item, "content", None)
            if reasoning:
                reasoning_parts.append(reasoning)
                await emit_thought_chunk(
                    orch_ctx,
                    text=reasoning,
                    author="assistant",
                    append=not first_reasoning,
                    last_chunk=False,
                    artifact_id=thought_id,
                )
                first_reasoning = False
            if content:
                content_parts.append(content)
                await emit_text_chunk(
                    orch_ctx,
                    text=content,
                    append=not first_content,
                    last_chunk=False,
                    artifact_id=text_id,
                )
                first_content = False
        reasoning = "".join(reasoning_parts)
        if reasoning:
            await emit_thought_chunk(
                orch_ctx,
                text=reasoning,
                author="assistant",
                append=False,
                last_chunk=True,
                artifact_id=thought_id,
            )
        content = "".join(content_parts)
        if content:
            await emit_text_chunk(
                orch_ctx,
                text=content,
                append=False,
                last_chunk=True,
                artifact_id=text_id,
            )
        if tool_call is None:
            raise PlanningFailed("planner stream ended without a plan")
        return tool_call

    async def _plan_and_launch(
        self,
        runtime: SessionRuntime,
        text: str,
        *,
        room: RoomOptions | None = None,
    ) -> None:
        state = runtime.state
        state.start_new_plan(f"plan-{uuid.uuid4().hex[:8]}")
        context_brief = await self._brief_builder.build(
            runtime.context_id, exclude_task_id=runtime.task_id
        )
        create_plan = create_plan_func
        ctx = FunctionContext(executor=self, runtime=runtime)
        try:
            tool_call = await self._stream_plan(
                runtime, text, ctx, context=context_brief or None
            )
        except PlanningFailed as exc:
            logger.warning("Planning failed for task %s: %s", runtime.task_id, exc)
            await self._persist(runtime)
            orch_ctx = self._build_ctx(runtime)
            await emit_function_error(
                orch_ctx, create_plan, str(exc),
                state_name=TaskState.TASK_STATE_FAILED,
            )
            self._session_mgr.evict_session(runtime.context_id)
            return

        draft = tool_call.args if isinstance(tool_call.args, PlanDraft) else (
            PlanDraft.model_validate(tool_call.args.model_dump())
        )

        if not draft.nodes:
            orch_ctx = self._build_ctx(runtime)
            await self._persist(runtime)
            await emit_event(orch_ctx, "", TaskState.TASK_STATE_COMPLETED)
            self._session_mgr.evict_session(runtime.context_id)
            return

        result = await tool_call.function.execute(ctx, tool_call.args)
        orch_ctx = self._build_ctx(runtime)
        await emit_function_call(
            orch_ctx, tool_call.function, tool_call.args, result,
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
                state.enqueue(
                    quoted_node.id, text, sender="user", quote_id=str(quote_id)
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
            invalidated = state.blocked_nodes()
            for blocked in invalidated:
                blocked.status = "invalidated"
            from choirworks.a2a.events import emit_state_delta as _esd
            ctx = self._build_ctx(runtime)
            await _esd(ctx, nodes={
                node.id: {"status": "canceled", "agent_name": node.agent_name},
                **{b.id: {"status": "invalidated"} for b in invalidated},
            })
            await self._spawn_followup_node(runtime, text, node, deps=[])
            return

        target = active[0] if active else next(
            (n for n in state.nodes.values() if n.status in {"pending", "ready"}),
            None,
        )
        if target is not None:
            state.enqueue(
                target.id,
                text,
                sender="user",
                quote_id=str(quote_id) if quote_id else None,
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
        if state.derived_count >= self._config.max_derived_nodes:
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
        ctx = self._build_ctx(runtime)
        await emit_state_delta(ctx, nodes={
            node_id: {
                "status": "pending",
                "agent_name": followup.agent_name,
            },
        })
        await self._persist(runtime)
        self._start_runner(runtime)

    # ------------------------------------------------------------ proxies
    # Backward-compatible proxy methods for tools that access
    # ``ctx.executor._xxx`` via FunctionContext.

    async def _emit_event(
        self,
        runtime: SessionRuntime,
        kind: str,
        state_name: TaskState = TaskState.TASK_STATE_WORKING,
        **metadata: Any,
    ) -> None:
        ctx = self._build_ctx(runtime)
        await emit_event(ctx, kind, state_name, **metadata)

    async def _persist(self, runtime: SessionRuntime) -> None:
        ctx = self._build_ctx(runtime)
        await self._session_mgr.persist(ctx)

    async def _join_members(
        self,
        runtime: SessionRuntime,
        names: list[str],
        reason: str,
    ) -> None:
        ctx = self._build_ctx(runtime)
        await join_members(ctx, names, reason)

    async def _apply_patch_locked(
        self, runtime: SessionRuntime, patch: PlanPatch
    ) -> Any:
        ctx = self._build_ctx(runtime)
        return await self._repair_mgr.apply_patch_locked(ctx, patch)
