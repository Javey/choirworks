from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any

from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import (
    create_agent_card_routes,
    create_jsonrpc_routes,
    create_rest_routes,
)
from a2a.server.routes.fastapi_routes import add_a2a_routes_to_fastapi
from a2a.server.tasks.database_task_store import DatabaseTaskStore
from a2a.types.a2a_pb2 import ListTasksRequest, TaskState
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from google.protobuf.json_format import MessageToDict
from sqlalchemy.ext.asyncio import create_async_engine

from choirworks.a2a.card import build_agent_card
from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.executor import ChoirWorksAgentExecutor
from choirworks.a2a.registry import AgentRegistry
from choirworks.api import agents as agents_routes
from choirworks.config import Settings
from choirworks.core.llm import LiteLLMClient
from choirworks.core.planner import Planner
from choirworks.core.policy import PolicyEngine
from choirworks.store.db import Database

logger = logging.getLogger(__name__)


async def create_app(
    settings: Settings | None = None, llm: LiteLLMClient | None = None
) -> FastAPI:
    settings = settings or Settings()
    llm_client = llm or LiteLLMClient(
        model=settings.llm.planner_model,
        api_base=settings.llm.api_base,
        timeout_seconds=settings.llm.timeout_seconds,
        context_window=settings.llm.context_window,
    )

    app = FastAPI(title="ChoirWorks", version="0.1.0")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db_path = settings.store.db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)

        engine = create_async_engine(
            f"sqlite+aiosqlite:///{db_path}",
            echo=False,
            connect_args={"check_same_thread": False},
        )
        task_store = DatabaseTaskStore(engine=engine, create_table=True)
        await task_store.initialize()

        # Use the existing Database wrapper for agent_registry compatibility
        db = Database(db_path)
        await db.initialize()

        remote = RemoteAgentClient()
        registry = AgentRegistry(db, remote)

        planner = Planner(
            llm_client,
            registry,
            max_nodes=settings.scheduler.max_plan_nodes,
            max_retries=settings.llm.max_plan_retries,
        )
        policy = PolicyEngine(settings.policies)

        executor = ChoirWorksAgentExecutor(
            registry=registry,
            remote=remote,
            planner=planner,
            policy=policy,
            llm=llm_client,
            max_parallel=settings.scheduler.max_parallel_nodes,
            node_timeout=settings.scheduler.node_timeout_seconds,
            max_node_attempts=settings.scheduler.max_node_attempts,
            retry_backoff=settings.scheduler.retry_backoff_seconds,
            replan_on_failure=settings.scheduler.replan_on_failure,
            compaction_threshold=settings.llm.compaction_threshold,
            compaction_retention=settings.llm.compaction_retention,
        )
        executor.set_task_store(task_store)

        agent_card = build_agent_card(settings.a2a.public_url)
        request_handler = DefaultRequestHandler(
            agent_executor=executor,
            task_store=task_store,
            agent_card=agent_card,
        )

        app.state.settings = settings
        app.state.engine = engine
        app.state.task_store = task_store
        app.state.db = db
        app.state.remote = remote
        app.state.registry = registry
        app.state.planner = planner
        app.state.policy = policy
        app.state.executor = executor
        app.state.request_handler = request_handler
        app.state.agent_card = agent_card

        # Mount A2A routes now that request_handler is available
        agent_card_routes = create_agent_card_routes(agent_card=agent_card)
        jsonrpc_routes = create_jsonrpc_routes(
            request_handler=request_handler, rpc_url="/v1/a2a"
        )
        rest_routes = create_rest_routes(request_handler=request_handler)
        add_a2a_routes_to_fastapi(
            app,
            agent_card_routes=agent_card_routes,
            jsonrpc_routes=jsonrpc_routes,
            rest_routes=rest_routes,
        )

        sim_agents: list[Any] = []
        if settings.sim.start_agents and settings.sim.agents:
            from choirworks.sim.fake_agent import start_fake_agent

            async with db.conn.execute("DELETE FROM agent_registry"):
                pass
            async with db.conn.execute("DELETE FROM tasks"):
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
            logger.info("Started %d sim agent(s)", len(sim_agents))

        from choirworks.a2a.recovery import recover_tasks

        if settings.recovery.replay_on_startup:
            await recover_tasks(request_handler, task_store)

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

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/conversations")
    async def create_conversation(body: dict) -> dict:
        conversation_id = uuid.uuid4().hex
        title = body.get("title", "新对话")
        return {"conversation_id": conversation_id, "title": title}

    @app.get("/v1/conversations")
    async def list_conversations(request: Request) -> list[dict]:
        task_store = request.app.state.task_store
        ctx = ServerCallContext()
        params = ListTasksRequest()
        response = await task_store.list(params, ctx)
        conversations = []
        for task in response.tasks:
            task_dict = MessageToDict(task, preserving_proto_field_name=True)
            title = "新对话"
            meta = task_dict.get("metadata", {})
            if meta.get("title"):
                title = meta["title"]
            state_name = TaskState.Name(task.status.state).replace("TASK_STATE_", "").lower()
            conversations.append({
                "id": task.id,
                "title": title,
                "created_at": "",
                "updated_at": "",
                "task_count": 1,
                "last_status": state_name,
            })
        return conversations

    frontend_dir = settings.server.frontend_dir

    @app.get("/")
    async def root():
        if (frontend_dir / "index.html").exists():
            from fastapi.responses import FileResponse
            return FileResponse(frontend_dir / "index.html")
        raise HTTPException(status_code=404, detail="frontend not built")

    if frontend_dir.exists() and (frontend_dir / "assets").exists():
        app.mount(
            "/assets",
            StaticFiles(directory=frontend_dir / "assets", html=True),
            name="assets",
        )

    return app
