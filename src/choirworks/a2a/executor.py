from __future__ import annotations

import asyncio

import structlog
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import (
    TaskState,
)

from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.wire import status_update
from choirworks.core.context import ContextBriefBuilder
from choirworks.core.llm import LiteLLMClient
from choirworks.orchestration.context import ExecutorConfig, OrchestrationContext
from choirworks.orchestration.flows.message import MessagePayload, message_flow
from choirworks.orchestration.registry import AgentRegistry
from choirworks.orchestration.session import SessionManager, SessionRuntime
from choirworks.orchestration.state import ACTIVE_NODE_STATUSES, NodeStatus
from choirworks.orchestration.transitions import apply_transition
from choirworks.store.contexts import ContextStore

logger = structlog.get_logger(__name__)


class ChoirWorksAgentExecutor(AgentExecutor):
    """ChoirWorks orchestration engine as an A2A AgentExecutor.

    The A2A Task is the aggregate root. Plan/node/member/intervention state is
    persisted as A2A event metadata merged into the Task snapshot by the SDK
    ``TaskManager`` + ``DatabaseTaskStore``.

    ``execute()`` routes each inbound message and returns quickly; node work
    runs in background runners, so multiple agents can work and chat at once.

    ``execute()`` is a thin A2A adapter: it boots the session, builds the
    orchestration context and lock, then delegates the whole inbound message to
    ``message_flow``; all recover logic lives in ``orchestration/recover.py``.
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

        runtime = await self._session_mgr.ensure_session(
            context.context_id, context.task_id, event_queue
        )
        payload = MessagePayload(context=context)
        async with runtime.lock:
            _ = await message_flow.run(self._build_ctx(runtime), payload)

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

    # ------------------------------------------------------------- context

    def _build_ctx(self, runtime: SessionRuntime) -> OrchestrationContext:
        """Build an OrchestrationContext for the given runtime."""
        return OrchestrationContext(
            runtime=runtime,
            registry=self._registry,
            remote=self._remote,
            llm=self._llm,
            sessions=self._session_mgr,
            config=self._config,
            brief_builder=self._brief_builder,
        )

    async def _persist(self, runtime: SessionRuntime) -> None:
        ctx = self._build_ctx(runtime)
        await ctx.sessions.persist(ctx)
