from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import structlog
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import (
    create_agent_card_routes,
    create_jsonrpc_routes,
    create_rest_routes,
)
from a2a.server.routes.fastapi_routes import add_a2a_routes_to_fastapi
from a2a.server.tasks.database_task_store import DatabaseTaskStore
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine

from choirworks.a2a.card import build_agent_card
from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.executor import ChoirWorksAgentExecutor
from choirworks.api import agents as agents_routes
from choirworks.api import conversations as conversations_routes
from choirworks.config import Settings
from choirworks.core.llm import LiteLLMClient
from choirworks.orchestration.registry import AgentRegistry
from choirworks.orchestration.session import SessionManager
from choirworks.store.contexts import ContextStore
from choirworks.store.db import Database

if TYPE_CHECKING:
    from choirworks.sim.fake_agent import FakeAgent

logger = structlog.get_logger(__name__)


async def create_app(settings: Settings | None = None, llm: LiteLLMClient | None = None) -> FastAPI:
    settings = settings or Settings()
    llm_client = llm or LiteLLMClient(
        model=settings.llm.planner_model,
        api_base=settings.llm.api_base,
        timeout_seconds=settings.llm.timeout_seconds,
        context_window=settings.llm.context_window,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db_path = settings.store.db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)

        engine = create_async_engine(
            f"sqlite+aiosqlite:///{db_path}",
            echo=False,
            connect_args={"check_same_thread": False, "timeout": 30},
        )
        task_store = DatabaseTaskStore(engine=engine, create_table=True)
        await task_store.initialize()

        # Use the existing Database wrapper for agent_registry compatibility
        db = Database(db_path)
        await db.initialize()
        context_store = ContextStore(db)

        remote = RemoteAgentClient()
        registry = AgentRegistry(db, remote)
        session_mgr = SessionManager(context_store)

        executor = ChoirWorksAgentExecutor(
            registry=registry,
            remote=remote,
            llm=llm_client,
            task_store=task_store,
            session_mgr=session_mgr,
            max_parallel=settings.scheduler.max_parallel_nodes,
            node_timeout=settings.scheduler.node_timeout_seconds,
            max_node_attempts=settings.scheduler.max_node_attempts,
            retry_backoff=settings.scheduler.retry_backoff_seconds,
            max_revisions=settings.scheduler.max_revisions,
            replan_on_failure=settings.scheduler.replan_on_failure,
            max_plan_nodes=settings.scheduler.max_plan_nodes,
            max_plan_retries=settings.llm.max_plan_retries,
            compaction_threshold=settings.llm.compaction_threshold,
            compaction_retention=settings.llm.compaction_retention,
        )

        agent_card = build_agent_card(settings.a2a.public_url)
        request_handler = DefaultRequestHandler(
            agent_executor=executor,
            task_store=task_store,
            agent_card=agent_card,
        )

        app.state.settings = settings
        app.state.engine = engine
        app.state.task_store = task_store
        app.state.context_store = context_store
        app.state.db = db
        app.state.remote = remote
        app.state.registry = registry
        app.state.session_mgr = session_mgr
        app.state.request_handler = request_handler
        app.state.agent_card = agent_card

        # Mount A2A routes now that request_handler is available
        agent_card_routes = create_agent_card_routes(agent_card=agent_card)
        jsonrpc_routes = create_jsonrpc_routes(request_handler=request_handler, rpc_url="/v1/a2a")
        rest_routes = create_rest_routes(request_handler=request_handler)
        add_a2a_routes_to_fastapi(
            app,
            agent_card_routes=agent_card_routes,
            jsonrpc_routes=jsonrpc_routes,
            rest_routes=rest_routes,
        )

        sim_agents: list[FakeAgent] = []
        if settings.sim.start_agents and settings.sim.agents:
            from choirworks.sim.fake_agent import start_fake_agent

            async with db.conn.execute("DELETE FROM agent_registry"):
                pass
            async with db.conn.execute("DELETE FROM tasks"):
                pass
            async with db.conn.execute("DELETE FROM contexts"):
                pass
            await db.conn.commit()
            for spec in settings.sim.agents:
                name = spec["name"]
                behavior = spec.get("behavior", "echo")
                agent = await start_fake_agent(
                    behavior,
                    name=name,
                    chunk_size=settings.sim.chunk_size,
                    chunk_delay=settings.sim.chunk_delay,
                )
                sim_agents.append(agent)
                await registry.register(name, agent.url)
            logger.info("Started sim agent(s)", count=len(sim_agents))

        from choirworks.a2a.recovery import recover_tasks

        if settings.recovery.replay_on_startup:
            await recover_tasks(request_handler, task_store, context_store)

        try:
            yield
        finally:
            for agent in sim_agents:
                await agent.stop()
            await executor.shutdown()
            await request_handler.aclose()
            await remote.close()
            await db.close()
            await engine.dispose()

    app = FastAPI(title="ChoirWorks", version="0.1.0", lifespan=lifespan)

    app.include_router(agents_routes.router, prefix="/v1")
    app.include_router(conversations_routes.router, prefix="/v1")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
