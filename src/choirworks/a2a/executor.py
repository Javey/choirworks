from __future__ import annotations

import asyncio
import re

import structlog
from a2a.helpers import new_task
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks.task_store import TaskStore
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types.a2a_pb2 import (
    TaskState,
)

from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.room import room_options
from choirworks.a2a.tasks import RECOVER_KEY
from choirworks.a2a.wire import QuestionResponse, parse_question_response, status_update
from choirworks.core.context import ContextBriefBuilder
from choirworks.core.llm import LiteLLMClient
from choirworks.orchestration.context import ExecutorConfig, OrchestrationContext
from choirworks.orchestration.events import (
    emit_intervention_rejected,
    emit_pending_questions,
    emit_state_delta,
)
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
    NodeStatus,
    has_pending_work,
    normalize_interventions,
    pending_interventions,
)
from choirworks.orchestration.transitions import apply_transition
from choirworks.store.contexts import ContextStore
from choirworks.tools.capabilities import ToolEffects

logger = structlog.get_logger(__name__)


def _is_recover_request(context: RequestContext) -> bool:
    return context.call_context.state.get(RECOVER_KEY) is True


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
        task_store: TaskStore,
        context_store: ContextStore,
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
            task_store=task_store,
            compaction_threshold=compaction_threshold,
            compaction_retention=compaction_retention,
        )

        self._session_mgr = SessionManager(context_store)
        self._registry = registry
        self._remote = remote
        self._llm = llm

    # ------------------------------------------------------------- lifecycle

    @property
    def _sessions(self) -> dict[str, SessionRuntime]:
        return self._session_mgr.sessions

    def session_is_active(self, context_id: str) -> bool:
        return self._session_mgr.session_is_active(context_id)

    def drop_session(self, context_id: str) -> None:
        self._session_mgr.drop_session(context_id)

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        """Route one inbound message; background runners do the actual work."""
        assert context.task_id is not None
        assert context.context_id is not None
        task_id = context.task_id
        context_id = context.context_id

        if _is_recover_request(context):
            logger.info("execute recover", task_id=task_id, context_id=context_id)
            await self._recover_task(task_id, context_id, event_queue)
            return

        text = (context.get_user_input() or "").strip()
        runtime = await self._session_mgr.ensure_session(context_id, task_id, event_queue)

        responses: list[QuestionResponse] = []
        malformed: str | None = None
        if context.message is not None:
            try:
                responses = parse_question_response(context.message)
            except ValueError as exc:
                malformed = str(exc)

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
            if malformed is not None:
                logger.info(
                    "execute route=intervention_malformed",
                    task_id=task_id,
                    context_id=context_id,
                )
                await emit_intervention_rejected(self._build_ctx(runtime), "", malformed)
                return
            if responses:
                logger.info(
                    "execute route=intervention_answers",
                    task_id=task_id,
                    context_id=context_id,
                    answers=len(responses),
                )
                orch_ctx = self._build_ctx(runtime)
                resolved = False
                for response in responses:
                    if await answer_intervention(
                        orch_ctx,
                        intervention_id=response.intervention_id,
                        answer=response.answer,
                    ):
                        resolved = True
                if resolved:
                    if pending_interventions(state):
                        await emit_pending_questions(orch_ctx)
                    runtime.wake.set()
                    self._start_runner(runtime)
                return

            if pending_interventions(state):
                logger.info(
                    "execute route=intervention_unanswered",
                    task_id=task_id,
                    context_id=context_id,
                    text_len=len(text),
                )
                await emit_pending_questions(self._build_ctx(runtime))
                return

            if runtime.runner is not None and not runtime.runner.done():
                logger.info(
                    "execute route=active_runner",
                    task_id=task_id,
                    context_id=context_id,
                    text_len=len(text),
                )
                await route_message(self._build_ctx(runtime), text, room)
                return

            if has_pending_work(state):
                logger.info(
                    "execute route=pending_work",
                    task_id=task_id,
                    context_id=context_id,
                    text_len=len(text),
                )
                await route_message(self._build_ctx(runtime), text, room)
                self._start_runner(runtime)
                return

            if not text:
                logger.info("execute route=empty_complete", task_id=task_id, context_id=context_id)
                await updater.complete()
                self._session_mgr.evict_session(context_id)
                return

            logger.info(
                "execute route=plan_and_launch",
                task_id=task_id,
                context_id=context_id,
                text_len=len(text),
            )
            await updater.start_work()
            await plan_and_launch(self._build_ctx(runtime), text, room=room)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        assert context.task_id is not None
        assert context.context_id is not None
        task_id = context.task_id
        context_id = context.context_id
        logger.info("cancel", task_id=task_id, context_id=context_id)
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
            canceled = 0
            for node in list(state.nodes.values()):
                if node.status in ACTIVE_NODE_STATUSES | {NodeStatus.READY}:
                    apply_transition(node, NodeStatus.CANCELED)
                    canceled += 1
            await self._persist(runtime)
            for node in list(state.nodes.values()):
                if node.status == NodeStatus.CANCELED and node.a2a_task_id:
                    await self._remote.cancel_task(node.agent_url, node.a2a_task_id)
            logger.info("cancel", task_id=task_id, context_id=context_id, canceled_nodes=canceled)
        self._session_mgr.evict_session(context_id)

    async def shutdown(self) -> None:
        runtimes = list(self._sessions.values())
        logger.info("shutdown", runtimes=len(runtimes))
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

    async def _recover_task(self, task_id: str, context_id: str, event_queue: EventQueue) -> None:
        # ensure_session 内部已从 context_store 加载 state 到 runtime.state。
        # recover_tasks 在调 on_message_send 前已保证 context_store 中存在
        # 该 context_id（recovery.py:55-57），所以 runtime.state 不会为空。
        runtime = await self._session_mgr.ensure_session(context_id, task_id, event_queue)
        logger.info("recover state loaded", task_id=task_id, context_id=context_id)
        await self._recover(runtime)

    async def _recover(self, runtime: SessionRuntime) -> None:
        # 计划修订（repair.py）会对活跃节点创建 confirm_cancel 干预，
        # 等待用户确认是否打断。进程崩溃后恢复时：
        # - 若目标节点在崩溃前已结束（不在 ACTIVE_NODE_STATUSES），
        #   该干预已无意义，立即标记为 expired 清理掉。
        # - 若目标节点仍为活跃状态，干预保留为 pending，由后续 runner
        #   重新挂接远程 task 后，通过 expire_cancel_requests 正常处理。
        expired = normalize_interventions(runtime.state)
        if expired:
            logger.info(
                "recover expired interventions",
                task_id=runtime.task_id,
                context_id=runtime.context_id,
                expired_interventions=len(expired),
            )
            ctx = self._build_ctx(runtime)
            await emit_state_delta(
                ctx,
                interventions={
                    iv.id: {
                        "status": "expired",
                        "node_id": iv.node_id,
                        "kind": iv.kind,
                    }
                    for iv in expired
                },
            )
        # 活跃节点重置为 recover（有远程 task 可重新订阅）或 pending（重新派发）。
        for node in runtime.state.nodes.values():
            if node.status in ACTIVE_NODE_STATUSES:
                apply_transition(
                    node,
                    NodeStatus.RECOVER if node.a2a_task_id else NodeStatus.PENDING,
                )
        logger.info(
            "recover nodes",
            task_id=runtime.task_id,
            context_id=runtime.context_id,
            nodes={n.id: n.status for n in runtime.state.nodes.values()},
        )
        await self._persist(runtime)
        self._start_runner(runtime)

    # ------------------------------------------------------------- context

    def _build_ctx(self, runtime: SessionRuntime) -> OrchestrationContext:
        """Build an OrchestrationContext for the given runtime."""
        effects = ToolEffects(
            max_derived_nodes=self._config.max_derived_nodes,
            join_members=lambda names, reason: self._join_members(runtime, names, reason),
            persist=lambda: self._persist(runtime),
            apply_patch_locked=lambda patch: self._apply_patch_locked(runtime, patch),
        )
        return OrchestrationContext(
            runtime=runtime,
            registry=self._registry,
            remote=self._remote,
            llm=self._llm,
            sessions=self._session_mgr,
            config=self._config,
            brief_builder=self._brief_builder,
            effects=effects,
        )

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

    async def _apply_patch_locked(self, runtime: SessionRuntime, patch: PlanPatch) -> PatchResult:
        ctx = self._build_ctx(runtime)
        return await apply_patch_locked(ctx, patch)
