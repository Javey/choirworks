from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.api import agents as agents_routes
from agent_hub.api import tasks as tasks_routes
from agent_hub.config import Settings
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.tasks import TaskService
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db = Database(resolved.store.db_path)
        await db.initialize()
        remote = RemoteAgentClient()
        event_store = EventStore(db)
        registry = AgentRegistry(db, remote)
        task_service = TaskService(db, event_store, registry)
        dispatcher = NodeDispatcher(
            db,
            event_store,
            remote,
            timeout_seconds=resolved.scheduler.node_timeout_seconds,
        )

        app.state.settings = resolved
        app.state.db = db
        app.state.remote = remote
        app.state.event_store = event_store
        app.state.registry = registry
        app.state.task_service = task_service
        app.state.dispatcher = dispatcher
        try:
            yield
        finally:
            await remote.close()
            await db.close()

    app = FastAPI(title="Agent Hub", version="0.1.0", lifespan=lifespan)
    app.include_router(tasks_routes.router, prefix="/v1")
    app.include_router(agents_routes.router, prefix="/v1")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
