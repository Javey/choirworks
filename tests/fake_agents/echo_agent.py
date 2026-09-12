from __future__ import annotations

import asyncio
import contextlib
import socket
from dataclasses import dataclass

import uvicorn
from a2a.helpers import get_message_text, new_task_from_user_message
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
    Part,
)
from starlette.applications import Starlette


class ScriptedExecutor(AgentExecutor):
    """可控行为的假 agent：

    - echo: 添加 artifact "echo:{text}" 后完成
    - ask:  先进入 input-required，收到后续消息后完成
    - fail: 直接失败
    - slow: 等待 5 秒后完成（用于超时测试）
    """

    def __init__(self, behavior: str = "echo"):
        self._behavior = behavior

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        text = get_message_text(context.message) if context.message else ""
        if context.current_task is None:
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)
            updater = TaskUpdater(event_queue, task.id, task.context_id)
            await updater.start_work()
            if self._behavior == "ask":
                await updater.requires_input(
                    updater.new_agent_message(parts=[Part(text="who are you?")])
                )
                return
            if self._behavior == "fail":
                await updater.failed(updater.new_agent_message(parts=[Part(text="boom")]))
                return
            if self._behavior == "slow":
                await asyncio.sleep(5)
            await updater.add_artifact(
                parts=[Part(text=f"echo:{text}")], name="response", last_chunk=True
            )
            await updater.complete()
        else:
            task = context.current_task
            updater = TaskUpdater(event_queue, task.id, task.context_id)
            await updater.add_artifact(
                parts=[Part(text=f"answered:{text}")],
                name="response",
                last_chunk=True,
            )
            await updater.complete()

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        updater = TaskUpdater(event_queue, context.task_id or "", context.context_id or "")
        await updater.cancel()


@dataclass
class FakeAgent:
    url: str
    card: AgentCard
    server: uvicorn.Server
    task: asyncio.Task
    handler: DefaultRequestHandler

    async def stop(self) -> None:
        await self.handler.aclose()
        self.server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.task, timeout=5)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _make_card(behavior: str, url: str) -> AgentCard:
    return AgentCard(
        name=f"fake-{behavior}",
        description=f"scripted test agent ({behavior})",
        version="1.0.0",
        capabilities=AgentCapabilities(streaming=True),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        skills=[
            AgentSkill(
                id="echo",
                name="echo",
                description="echoes input",
                tags=["test"],
            )
        ],
        supported_interfaces=[
            AgentInterface(protocol_binding="JSONRPC", url=url, protocol_version="1.0")
        ],
    )


async def start_fake_agent(behavior: str = "echo") -> FakeAgent:
    port = _free_port()
    url = f"http://127.0.0.1:{port}"
    card = _make_card(behavior, url)
    handler = DefaultRequestHandler(
        agent_executor=ScriptedExecutor(behavior),
        task_store=InMemoryTaskStore(),
        agent_card=card,
    )
    routes = create_agent_card_routes(agent_card=card) + create_jsonrpc_routes(
        request_handler=handler, rpc_url="/"
    )
    app = Starlette(routes=routes)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - 轮询 uvicorn 启动状态，无事件可用
        await asyncio.sleep(0.02)
    return FakeAgent(url=url, card=card, server=server, task=task, handler=handler)
