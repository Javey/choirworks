from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

from choirworks.a2a.card import build_agent_card
from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.reconcile import reconcile_once
from choirworks.a2a.registry import AgentRegistry
from choirworks.a2a.server import HubA2AHandler
from choirworks.api import agents as agents_routes
from choirworks.api import conversations as conversations_routes
from choirworks.api import interventions as interventions_routes
from choirworks.api import messages as messages_routes
from choirworks.api import rollback as rollback_routes
from choirworks.api import sse as sse_routes
from choirworks.api import tasks as tasks_routes
from choirworks.config import Settings
from choirworks.core.coordinator import RoomCoordinator
from choirworks.core.dispatcher import NodeDispatcher
from choirworks.core.events import EventBus
from choirworks.core.llm import LiteLLMClient, LLMClient
from choirworks.core.orchestrator import Orchestrator
from choirworks.core.planner import Planner
from choirworks.core.policy import PolicyEngine
from choirworks.core.recovery import RecoveryResult, recover_tasks
from choirworks.core.tasks import TaskService
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore

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
        coordinator = RoomCoordinator(
            db,
            event_store,
            task_service,
            orchestrator,
            registry,
            llm=llm_client,
            remote=remote,
        )
        orchestrator.set_coordinator(coordinator)

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
        app.state.coordinator = coordinator

        recovery: RecoveryResult | None = None
        reconcile_task: asyncio.Task | None = None
        if resolved.recovery.replay_on_startup:
            recovery = await recover_tasks(
                db, event_store, remote, dispatcher, orchestrator
            )
            app.state.recovery = recovery
            if recovery.background:
                asyncio.gather(*recovery.background, return_exceptions=True)
            try:
                await coordinator.reconcile()
            except Exception:  # noqa: BLE001 - 对账失败不阻塞启动
                logger.exception("room coordinator reconcile failed")
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

    app = FastAPI(title="ChoirWorks", version="0.1.0", lifespan=lifespan)
    app.include_router(tasks_routes.router, prefix="/v1")
    app.include_router(conversations_routes.router, prefix="/v1")
    app.include_router(messages_routes.router, prefix="/v1")
    app.include_router(agents_routes.router, prefix="/v1")
    app.include_router(sse_routes.router, prefix="/v1")
    app.include_router(interventions_routes.router, prefix="/v1")
    app.include_router(rollback_routes.router, prefix="/v1")

    card = build_agent_card(resolved.a2a.public_url)
    a2a_handler = HubA2AHandler(app)
    app.router.routes.extend(
        create_agent_card_routes(agent_card=card)
        + create_jsonrpc_routes(request_handler=a2a_handler, rpc_url="/v1/a2a")
    )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    frontend_dir = resolved.server.frontend_dir
    if (frontend_dir / "index.html").exists():
        app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
    else:

        @app.get("/")
        async def root() -> dict[str, str]:
            raise HTTPException(status_code=404, detail="frontend not built")

    return app
