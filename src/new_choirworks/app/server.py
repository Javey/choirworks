from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

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

from new_choirworks.a2a.card import build_agent_card
from new_choirworks.a2a.executor import BaseAgentExecutor
from new_choirworks.config import Settings
from new_choirworks.core.agents.base_agent import BaseAgent
from new_choirworks.core.agents.llm_agent import LLMAgent
from new_choirworks.core.models.lite_llm import LiteLLM
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.orchestrator import OrchestratorAgent


async def create_app(settings: Settings) -> FastAPI:
    """Assemble a FastAPI app exposing the agent via A2A protocol.

    配置了 orchestrator.agents 则服务编排 Agent（DAG 多 agent 协作），
    否则退化为单体 LLMAgent 助手。
    """

    llm = LiteLLM(
        model=settings.llm.model,
        base_url=settings.llm.api_base,
    )
    agent: BaseAgent
    if settings.orchestrator.agents:
        agent = OrchestratorAgent(
            name="choirworks",
            agents=[
                AgentInfo(name=a.name, url=a.url, description=a.description)
                for a in settings.orchestrator.agents
            ],
            llm=llm,
        )
    else:
        agent = LLMAgent(
            name="choirworks",
            llm=llm,
            instruction="You are a helpful assistant.",
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        db_path = settings.store.db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{db_path}",
            echo=False,
            connect_args={"check_same_thread": False, "timeout": 30},
        )

        task_store = DatabaseTaskStore(engine=engine, create_table=True)
        await task_store.initialize()

        executor = BaseAgentExecutor(agent)
        agent_card = build_agent_card(settings.a2a.public_url)
        request_handler = DefaultRequestHandler(
            agent_executor=executor,
            task_store=task_store,
            agent_card=agent_card,
        )

        agent_card_routes = create_agent_card_routes(agent_card=agent_card)
        jsonrpc_routes = create_jsonrpc_routes(request_handler=request_handler, rpc_url="/v1/a2a")
        rest_routes = create_rest_routes(request_handler=request_handler)
        add_a2a_routes_to_fastapi(
            app,
            agent_card_routes=agent_card_routes,
            jsonrpc_routes=jsonrpc_routes,
            rest_routes=rest_routes,
        )

        try:
            yield
        finally:
            await request_handler.aclose()
            await engine.dispose()

    app = FastAPI(title="ChoirWorks Agent", version="1.0.0", lifespan=lifespan)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
