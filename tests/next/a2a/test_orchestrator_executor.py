from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from typing import Protocol, cast, override

from a2a.helpers import new_text_message
from a2a.server.agent_execution import RequestContext
from a2a.server.context import ServerCallContext
from a2a.server.events import Event, EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types.a2a_pb2 import (
    Role,
    SendMessageRequest,
    Task,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatusUpdateEvent,
)

from new_choirworks.a2a.card import build_agent_card
from new_choirworks.a2a.executor import BaseAgentExecutor
from new_choirworks.core.models.llm_request import LLMRequest
from new_choirworks.core.models.llm_response import LLMResponse
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.orchestrator import OrchestratorAgent
from new_choirworks.orchestrator.plan import DagPlan, PlanNode

from ..conftest import FakeLLM
from ..orchestrator.test_orchestrator import _chunk, _ScriptRunner


class _StreamingRequestHandler(Protocol):
    def on_message_send_stream(
        self, params: SendMessageRequest, context: ServerCallContext
    ) -> AsyncGenerator[Event, None]: ...


def _PlannerLLM(plans: list[str]) -> FakeLLM:
    def responder(i: int, _req: LLMRequest) -> list[LLMResponse]:
        return [_chunk(plans[min(i, len(plans) - 1)])]

    return FakeLLM(responder=responder)


def _plan_json(*nodes: PlanNode) -> str:
    return json.dumps(DagPlan(nodes=list(nodes)).model_dump())


async def test_orchestrator_served_over_a2a():
    """Server 级：OrchestratorAgent + BaseAgentExecutor + DefaultRequestHandler
    + InMemoryTaskStore → on_message_send_stream（fake LLM / fake runner，零网络）。"""
    node = PlanNode(id="a", name="a", agent_name="writer", instruction="write")
    plans = [
        _plan_json(node),
        _plan_json(),
    ]

    def responder(i: int, _req: LLMRequest) -> list[LLMResponse]:
        return [_chunk(plans[min(i, len(plans) - 1)])]

    log: list[str] = []
    runner = _ScriptRunner({"writer": ["plain output"]}, log)
    agent = OrchestratorAgent(
        name="choirworks",
        agents=[AgentInfo(name="writer", url="http://writer")],
        llm=FakeLLM(responder=responder),
        node_runner=runner,
    )
    handler = DefaultRequestHandler(
        agent_executor=BaseAgentExecutor(agent),
        task_store=InMemoryTaskStore(),
        agent_card=build_agent_card("http://localhost:8080"),
    )
    request_handler = cast(_StreamingRequestHandler, handler)

    try:
        request = SendMessageRequest(
            message=new_text_message("do something", role=Role.ROLE_USER),
        )
        gen = request_handler.on_message_send_stream(request, ServerCallContext())
        events = [e async for e in gen]

        tasks = [e for e in events if isinstance(e, Task)]
        assert len(tasks) == 1

        status_updates = [e for e in events if isinstance(e, TaskStatusUpdateEvent)]
        assert status_updates
        assert status_updates[-1].status.state == TaskState.TASK_STATE_COMPLETED

        # 编排的 A2A 节点事件经共享 event_queue 流出
        assert runner.log == ["run:a"]
        artifacts = [e for e in events if isinstance(e, TaskArtifactUpdateEvent)]
        assert artifacts
    finally:
        await handler.aclose()


class _Queue(EventQueue):
    def __init__(self) -> None:
        self.events: list[object] = []

    @override
    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


def _ctx(task_id: str = "t1") -> RequestContext:
    return RequestContext(
        call_context=ServerCallContext(),
        request=SendMessageRequest(message=new_text_message("do something", role=Role.ROLE_USER)),
        task_id=task_id,
        context_id="c1",
    )


async def test_cancel_stops_running_execute():
    """评审 P1-3：cancel 真正取消正在运行的 execute（触发 agent 清理），再发 CANCELED。"""
    log: list[str] = []
    runner = _ScriptRunner({"writer": ["x"]}, log, delays={"a": 10.0})
    llm_responder = _PlannerLLM(
        [
            _plan_json(PlanNode(id="a", name="a", agent_name="writer", instruction="write")),
            _plan_json(),
        ]
    )
    agent = OrchestratorAgent(
        name="choirworks",
        agents=[AgentInfo(name="writer", url="http://writer")],
        llm=llm_responder,
        node_runner=runner,
    )
    executor = BaseAgentExecutor(agent)
    queue = _Queue()
    request_ctx = _ctx()

    exec_task = asyncio.create_task(executor.execute(request_ctx, queue))
    _ = await asyncio.sleep(0.05)  # a 已派发并在途
    assert [n for n, _ in runner.messages] == ["a"]

    await executor.cancel(request_ctx, queue)
    _ = await asyncio.wait_for(exec_task, timeout=2.0)

    assert runner.cancelled == ["a"]  # agent finally → runtime.shutdown 生效
    statuses = [e for e in queue.events if isinstance(e, TaskStatusUpdateEvent)]
    assert statuses[-1].status.state == TaskState.TASK_STATE_CANCELED


async def test_agent_exception_yields_failed_status():
    """评审 P2-6：agent 异常 → FAILED 终态（错误文本），不发 COMPLETED、不静默。"""
    from new_choirworks.core.agents.base_agent import BaseAgent
    from new_choirworks.core.agents.invocation_context import InvocationContext
    from new_choirworks.core.events.event import A2AEvent

    class _BoomAgent(BaseAgent):
        @override
        async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
            raise RuntimeError("agent exploded")
            yield  # pyright: ignore[reportUnreachable]

    executor = BaseAgentExecutor(_BoomAgent(name="boom"))
    queue = _Queue()
    await executor.execute(_ctx(), queue)

    statuses = [e for e in queue.events if isinstance(e, TaskStatusUpdateEvent)]
    assert statuses[-1].status.state == TaskState.TASK_STATE_FAILED
    assert "agent exploded" in str(statuses[-1])
    assert not any(e.status.state == TaskState.TASK_STATE_COMPLETED for e in statuses)
