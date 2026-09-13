from __future__ import annotations

import asyncio
import contextlib
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

from agent_hub.sim.ports import free_port

LEGACY_BEHAVIORS = {"echo", "ask", "fail", "fail_once", "slow", "delay"}


class ScriptedExecutor(AgentExecutor):
    """可控行为的假 agent。

    测试行为（保持兼容）：
    - echo: 添加 artifact "echo:{text}" 后完成
    - ask:  先进入 input-required，收到后续消息后完成
    - fail: 直接失败
    - fail_once: 首次失败，之后成功
    - slow: 等待 5 秒后完成（用于超时测试）
    - delay: 等待 0.4 秒后完成

    模拟演示行为：
    - research: 等待 1.2 秒，返回「调研结果…」
    - write:    返回「文稿…」
    - review:   进入 input-required「请确认是否采用？」；收到答复后「已定稿」
    - flaky_once:   首次失败，之后成功（自动重试演示）
    - flaky_always: 始终失败（触发重规划演示）
    """

    def __init__(
        self,
        behavior: str = "echo",
        name: str = "",
        *,
        chunk_size: int = 0,
        chunk_delay: float = 0.0,
    ):
        self._behavior = behavior
        self._name = name
        self._calls = 0
        self._chunk_size = chunk_size
        self._chunk_delay = chunk_delay

    async def _emit_artifact(self, updater: TaskUpdater, text: str) -> None:
        if self._chunk_size <= 0:
            await updater.add_artifact(
                parts=[Part(text=text)], name="response", last_chunk=True
            )
            return
        from uuid import uuid4

        artifact_id = uuid4().hex
        chunks = [
            text[index : index + self._chunk_size]
            for index in range(0, len(text), self._chunk_size)
        ] or [""]
        for index, piece in enumerate(chunks):
            await updater.add_artifact(
                parts=[Part(text=piece)],
                artifact_id=artifact_id,
                name="response",
                append=index > 0,
                last_chunk=index == len(chunks) - 1,
            )
            if self._chunk_delay > 0 and index < len(chunks) - 1:
                await asyncio.sleep(self._chunk_delay)

    def _question_text(self) -> str:
        if self._behavior == "review":
            return "请确认是否采用该方案？"
        return "who are you?"

    def _success_text(self, text: str) -> str:
        if self._behavior == "research":
            return f"调研结果（{self._name or 'researcher'}）：关于「{text}」的模拟要点。"
        if self._behavior == "write":
            return f"文稿（{self._name or 'writer'}）：基于「{text}」生成的模拟报告。"
        return f"echo:{text}"

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        text = get_message_text(context.message) if context.message else ""
        if context.current_task is None:
            self._calls += 1
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)
            updater = TaskUpdater(event_queue, task.id, task.context_id)
            await updater.start_work()
            if self._behavior in ("ask", "review"):
                await updater.requires_input(
                    updater.new_agent_message(parts=[Part(text=self._question_text())])
                )
                return
            if self._behavior in ("fail", "flaky_always") or (
                self._behavior in ("fail_once", "flaky_once") and self._calls == 1
            ):
                await updater.failed(updater.new_agent_message(parts=[Part(text="boom")]))
                return
            if self._behavior == "slow":
                await asyncio.sleep(5)
            if self._behavior == "research":
                await asyncio.sleep(1.2)
            if self._behavior == "delay":
                await asyncio.sleep(0.4)
            await self._emit_artifact(updater, self._success_text(text))
            await updater.complete()
        else:
            task = context.current_task
            updater = TaskUpdater(event_queue, task.id, task.context_id)
            if self._behavior == "review":
                answer = f"已按你的意见定稿：{text}"
            else:
                answer = f"answered:{text}"
            await self._emit_artifact(updater, answer)
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
        # sse-starlette 的 AppStatus.should_exit 是进程级全局变量，watcher 通过
        # SIGTERM handler 反射 uvicorn Server；测试进程内多个 server 顺序启停时
        # 旧 server 的退出会污染该标志，导致后续 SSE 流被提前终止。这里重置。
        from sse_starlette.sse import AppStatus

        AppStatus.should_exit = False


def _make_card(behavior: str, url: str, name: str = "") -> AgentCard:
    card_name = name or f"fake-{behavior}"
    return AgentCard(
        name=card_name,
        description=f"simulated A2A agent ({behavior})",
        version="1.0.0",
        capabilities=AgentCapabilities(streaming=True),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        skills=[
            AgentSkill(
                id="echo",
                name="echo",
                description="模拟回复",
                tags=["simulation"],
            )
        ],
        supported_interfaces=[
            AgentInterface(protocol_binding="JSONRPC", url=url, protocol_version="1.0")
        ],
    )


async def start_fake_agent(
    behavior: str = "echo",
    name: str = "",
    *,
    chunk_size: int = 0,
    chunk_delay: float = 0.0,
) -> FakeAgent:
    port = free_port()
    url = f"http://127.0.0.1:{port}"
    card = _make_card(behavior, url, name)
    handler = DefaultRequestHandler(
        agent_executor=ScriptedExecutor(
            behavior, name, chunk_size=chunk_size, chunk_delay=chunk_delay
        ),
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
