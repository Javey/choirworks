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

from choirworks.sim.ports import free_port

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
        if self._behavior == "collaborate":
            return "需要 qa-engineer 协助确认技术细节，请协助。"
        if self._behavior == "inquire":
            return "缺少关键信息：请 product-manager 提供需求文档。"
        return "who are you?"

    def _success_text(self, text: str) -> str:
        if self._behavior == "research":
            return f"调研结果：关于「{text}」的要点分析。"
        if self._behavior == "write":
            return f"已基于「{text}」完成接口实现和单元测试。"
        if self._behavior == "collaborate":
            if "请补充信息" in text:
                return "调研补充：结合 qa-engineer 的反馈，补充了技术可行性分析。"
            return f"调研结果：关于「{text}」的要点分析。"
        if self._behavior == "inquire":
            return f"已基于「{text}」完成接口实现和单元测试。"
        if self._behavior == "assist":
            return "分析结论：测试通过，功能符合预期。"
        return f"echo:{text}"

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        text = get_message_text(context.message) if context.message else ""
        instruction = (
            text.split("[当前任务]")[-1].strip() if "[当前任务]" in text else text
        )
        if context.current_task is None:
            self._calls += 1
            task = new_task_from_user_message(context.message)
            await event_queue.enqueue_event(task)
            updater = TaskUpdater(event_queue, task.id, task.context_id)
            await updater.start_work()
            if self._behavior in ("ask", "review") or (
                self._behavior in ("collaborate", "inquire")
                and "协作" in instruction
                and "请补充信息" not in instruction
                and self._calls == 1
            ):
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
            if self._behavior == "collaborate" and "请补充信息" in instruction:
                await asyncio.sleep(0.6)
            if self._behavior == "assist":
                await asyncio.sleep(0.5)
            if self._behavior == "delay":
                await asyncio.sleep(0.4)
            output_text = (
                instruction
                if self._behavior
                in ("research", "write", "collaborate", "inquire", "review", "assist")
                else text
            )
            await self._emit_artifact(updater, self._success_text(output_text))
            await updater.complete()
        else:
            task = context.current_task
            updater = TaskUpdater(event_queue, task.id, task.context_id)
            if self._behavior == "review":
                answer = f"已按你的意见定稿：{instruction}"
            elif self._behavior == "collaborate":
                answer = "协作完成：已结合 qa-engineer 的协助，方案确认可行。"
            elif self._behavior == "inquire":
                answer = f"已获得需求信息并完成开发：{instruction}"
            else:
                answer = f"answered:{instruction}"
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


BEHAVIOR_DESCRIPTIONS: dict[str, str] = {
    "collaborate": "负责需求分析、产品规划，协调团队成员推进项目",
    "inquire": "负责后端服务开发，实现 API 和业务逻辑",
    "review": "负责代码审查，确保代码质量和安全性",
    "assist": "负责测试和验证，确保功能符合需求",
    "echo": "负责财务数据分析，生成财务报告和预算建议",
    "flaky_once": "负责审批流程管理，处理报销和采购审批",
    "flaky_always": "负责合规审计，检查财务记录和流程合规性",
    "research": "负责需求分析、产品规划，协调团队成员推进项目",
    "write": "负责后端服务开发，实现 API 和业务逻辑",
}

BEHAVIOR_SKILL_DESCRIPTIONS: dict[str, str] = {
    "collaborate": "需求分析与产品规划",
    "inquire": "后端开发与接口实现",
    "review": "代码审查与质量把控",
    "assist": "软件测试与质量验证",
    "echo": "财务分析与报告生成",
    "flaky_once": "审批流程管理",
    "flaky_always": "合规审计与风险检查",
    "research": "需求分析与产品规划",
    "write": "后端开发与接口实现",
}


def _make_card(behavior: str, url: str, name: str = "") -> AgentCard:
    card_name = name or behavior
    skill_name = name or behavior
    description = BEHAVIOR_DESCRIPTIONS.get(behavior, f"{behavior} agent")
    skill_desc = BEHAVIOR_SKILL_DESCRIPTIONS.get(behavior, behavior)
    return AgentCard(
        name=card_name,
        description=description,
        version="1.0.0",
        capabilities=AgentCapabilities(streaming=True),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        skills=[
            AgentSkill(
                id=skill_name,
                name=skill_name,
                description=skill_desc,
                tags=[],
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
