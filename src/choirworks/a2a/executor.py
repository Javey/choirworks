from __future__ import annotations

import asyncio
import logging
import re

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
from choirworks.a2a.room import room_options
from choirworks.a2a.wire import status_update
from choirworks.core.context import ContextBriefBuilder
from choirworks.core.llm import LiteLLMClient
from choirworks.orchestration.context import ExecutorConfig, OrchestrationContext
from choirworks.orchestration.deps import Deps
from choirworks.orchestration.events import emit_event, emit_state_delta
from choirworks.orchestration.flows import join_members
from choirworks.orchestration.intervention import answer_intervention
from choirworks.orchestration.patch import PatchResult, PlanPatch
from choirworks.orchestration.planning import plan_and_launch
from choirworks.orchestration.registry import AgentRegistry
from choirworks.orchestration.repair import apply_patch_locked
from choirworks.orchestration.routing import route_message
from choirworks.orchestration.runner import start_runner
from choirworks.orchestration.session import SessionManager, SessionRuntime
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    has_pending_work,
    normalize_cancel_requests,
    pending_interventions,
)
from choirworks.store.contexts import ContextStore
from choirworks.tools.capabilities import ToolEffects

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

    This class is a thin A2A protocol layer: it wires the long-lived
    collaborators into :class:`Deps` and delegates all orchestration work to
    module-level functions.
    """

    def __init__(
        self,
        registry: AgentRegistry,
        remote: RemoteAgentClient,
        llm: LiteLLMClient,
        *,
        max_parallel: int = 5,
        node_timeout: float = 600.0,
        max_node_attempts: int = 2,
        retry_backoff: float = 1.0,
        max_derived_nodes: int = 5,
        max_revisions: int = 3,
        replan_on_failure: bool = True,
        max_plan_nodes: int = 20,
        max_plan_retries: int = 2,
        compaction_threshold: float = 0.8,
        compaction_retention: int = 10,
    ):
        self._config = ExecutorConfig(
            max_parallel=max_parallel,
            node_timeout=node_timeout,
            max_node_attempts=max_node_attempts,
            retry_backoff=retry_backoff,
            max_derived_nodes=max_derived_nodes,
            max_revisions=max_revisions,
            replan_on_failure=replan_on_failure,
            max_nodes=max_plan_nodes,
            max_plan_retries=max_plan_retries,
        )

        self._brief_builder = ContextBriefBuilder(
            llm,
            compaction_threshold=compaction_threshold,
            compaction_retention=compaction_retention,
        )

        self._session_mgr = SessionManager()
        self._deps = Deps(
            registry=registry,
            remote=remote,
            llm=llm,
            sessions=self._session_mgr,
            config=self._config,
            brief_builder=self._brief_builder,
        )

    # ------------------------------------------------------------- lifecycle

    def set_task_store(self, task_store: TaskStore) -> None:
        self._brief_builder.set_task_store(task_store)

    def set_context_store(self, context_store: ContextStore) -> None:
        self._session_mgr.set_context_store(context_store)

    @property
    def _sessions(self) -> dict[str, SessionRuntime]:
        return self._session_mgr.sessions

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
            if pending_interventions(state):
                if text:
                    orch_ctx = self._build_ctx(runtime)
                    await answer_intervention(orch_ctx, text)
                    if getattr(runtime, "runner_start_requested", False):
                        runtime.runner_start_requested = False
                        self._start_runner(runtime)
                return

            if runtime.runner is not None and not runtime.runner.done():
                await route_message(self._build_ctx(runtime), text, room)
                return

            if has_pending_work(state):
                await route_message(self._build_ctx(runtime), text, room)
                self._start_runner(runtime)
                return

            if not text:
                await updater.complete()
                self._session_mgr.evict_session(context_id)
                return

            await updater.start_work()
            await plan_and_launch(self._build_ctx(runtime), text, room=room)

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
                    await self._deps.remote.cancel_task(node.agent_url, node.a2a_task_id)
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
        expired = normalize_cancel_requests(runtime.state)
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

    # ------------------------------------------------------------- context

    def _build_ctx(self, runtime: SessionRuntime) -> OrchestrationContext:
        """Build an OrchestrationContext for the given runtime."""
        runtime.runner_start_requested = False
        effects = ToolEffects(
            max_derived_nodes=self._config.max_derived_nodes,
            join_members=lambda names, reason: self._join_members(
                runtime, names, reason
            ),
            persist=lambda: self._persist(runtime),
            apply_patch_locked=lambda patch: self._apply_patch_locked(runtime, patch),
        )
        return OrchestrationContext(runtime=runtime, deps=self._deps, effects=effects)

    def _start_runner(self, runtime: SessionRuntime) -> None:
        start_runner(self._build_ctx(runtime))

    # ------------------------------------------------------- effect bindings
    # ToolEffects closures bind a runtime to the module-level functions, so
    # tools never see the executor.

    async def _persist(self, runtime: SessionRuntime) -> None:
        ctx = self._build_ctx(runtime)
        await ctx.sessions.persist(ctx)

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
    ) -> PatchResult:
        ctx = self._build_ctx(runtime)
        return await apply_patch_locked(ctx, patch)
