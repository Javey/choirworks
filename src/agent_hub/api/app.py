from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.reconcile import reconcile_once
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.api import agents as agents_routes
from agent_hub.api import interventions as interventions_routes
from agent_hub.api import rollback as rollback_routes
from agent_hub.api import sse as sse_routes
from agent_hub.api import tasks as tasks_routes
from agent_hub.config import Settings
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.events import EventBus
from agent_hub.core.llm import LiteLLMClient, LLMClient
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.core.planner import Planner
from agent_hub.core.policy import PolicyEngine
from agent_hub.core.recovery import RecoveryResult, recover_tasks
from agent_hub.core.tasks import TaskService
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore

logger = logging.getLogger(__name__)


async def _reconcile_loop(db, events, remote, interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await reconcile_once(db, events, remote)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - 对账失败不影响主流程
            logger.exception("reconcile failed")


def create_app(settings: Settings | None = None, llm: LLMClient | None = None) -> FastAPI:
    resolved = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db = Database(resolved.store.db_path)
        await db.initialize()
        remote = RemoteAgentClient()
        bus = EventBus()
        event_store = EventStore(db, bus=bus)
        registry = AgentRegistry(db, remote)
        task_service = TaskService(db, event_store, registry)
        dispatcher = NodeDispatcher(
            db,
            event_store,
            remote,
            timeout_seconds=resolved.scheduler.node_timeout_seconds,
        )
        llm_client = llm or LiteLLMClient(
            model=resolved.llm.planner_model,
            timeout_seconds=resolved.llm.timeout_seconds,
        )
        planner = Planner(
            llm_client,
            registry,
            max_nodes=resolved.scheduler.max_plan_nodes,
            max_retries=resolved.llm.max_plan_retries,
        )
        orchestrator = Orchestrator(
            db,
            event_store,
            planner,
            dispatcher,
            task_service,
            registry=registry,
            remote=remote,
            llm=llm_client,
            policy_engine=PolicyEngine(resolved.policies),
            max_parallel=resolved.scheduler.max_parallel_nodes,
            max_node_attempts=resolved.scheduler.max_node_attempts,
            retry_backoff_seconds=resolved.scheduler.retry_backoff_seconds,
            replan_on_failure=resolved.scheduler.replan_on_failure,
        )

        app.state.settings = resolved
        app.state.db = db
        app.state.remote = remote
        app.state.event_bus = bus
        app.state.event_store = event_store
        app.state.registry = registry
        app.state.task_service = task_service
        app.state.dispatcher = dispatcher
        app.state.planner = planner
        app.state.orchestrator = orchestrator

        recovery: RecoveryResult | None = None
        reconcile_task: asyncio.Task | None = None
        if resolved.recovery.replay_on_startup:
            recovery = await recover_tasks(
                db, event_store, remote, dispatcher, orchestrator
            )
            app.state.recovery = recovery
            if recovery.background:
                asyncio.gather(*recovery.background, return_exceptions=True)
        reconcile_task = asyncio.create_task(
            _reconcile_loop(
                db,
                event_store,
                remote,
                resolved.recovery.reconcile_interval_seconds,
            )
        )
        app.state.reconcile_task = reconcile_task
        try:
            yield
        finally:
            reconcile_task.cancel()
            await asyncio.gather(reconcile_task, return_exceptions=True)
            if recovery is not None:
                for task in recovery.background:
                    task.cancel()
                if recovery.background:
                    await asyncio.gather(
                        *recovery.background, return_exceptions=True
                    )
            await orchestrator.stop()
            await remote.close()
            await db.close()

    app = FastAPI(title="Agent Hub", version="0.1.0", lifespan=lifespan)
    app.include_router(tasks_routes.router, prefix="/v1")
    app.include_router(agents_routes.router, prefix="/v1")
    app.include_router(sse_routes.router, prefix="/v1")
    app.include_router(interventions_routes.router, prefix="/v1")
    app.include_router(rollback_routes.router, prefix="/v1")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
