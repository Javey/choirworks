from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Protocol, cast

from a2a.helpers import new_text_message
from a2a.server.context import ServerCallContext
from a2a.server.events import Event
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
