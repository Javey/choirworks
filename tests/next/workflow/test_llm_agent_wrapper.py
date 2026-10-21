from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import cast, override

import pytest
from a2a.server.events import EventQueue
from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent
from litellm.types.llms.openai import OpenAIChatCompletionUserMessage

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.agents.llm_agent import LLMAgent
from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.models.llm_request import LLMRequest
from new_choirworks.core.models.llm_response import (
    LLMResponse,
    LLMResponseContent,
    LLMResponsePart,
)
from new_choirworks.core.workflow.base_node import START
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.graph import Edge
from new_choirworks.core.workflow.workflow import Workflow

from ..conftest import FakeLLM


def _chunk(text: str, thought: bool = False) -> LLMResponse:
    return LLMResponse(
        content=LLMResponseContent(parts=[LLMResponsePart(text=text, thought=thought)])
    )


class _FakeEventQueue(EventQueue):
    """收集事件用于断言。"""

    def __init__(self) -> None:
        self._events: list[object] = []

    @override
    async def enqueue_event(self, event: object) -> None:
        self._events.append(event)

    @property
    def events(self) -> list[object]:
        return self._events


@pytest.mark.asyncio
async def test_workflow_with_llm_agent_node():
    """LLMAgent 作为 workflow 节点：完整链路从 Workflow.run 到 ctx.output。"""
    llm = FakeLLM(responder=lambda i, r: [_chunk("hello"), _chunk(" world")])
    agent = LLMAgent(name="test", llm=llm, include_contents="none")
    wf = Workflow(name="wf", edges=[Edge(START, agent)])

    queue = _FakeEventQueue()
    ic = InvocationContext(event_queue=queue, task_id="t1", context_id="c1")
    ctx = Context(invocation_context=ic, node=wf)

    async for _ in wf.run(ctx):
        pass

    assert ctx.output == "hello world"
    assert len(queue.events) == 3
    tracked = [e for e in ic.events if isinstance(e, TaskArtifactUpdateEvent)]
    assert len(tracked) == 1
    assert tracked[0].last_chunk is True
    assert tracked[0].append is False


@pytest.mark.asyncio
async def test_workflow_llm_agent_chain():
    """两节点链：A 的 output 作为 B 的 node_input 注入。"""
    llm_a = FakeLLM(responder=lambda i, r: [_chunk("hello")])
    captured_b: list[LLMRequest] = []

    class CaptureLLM(BaseLLM):
        model: str = "fake"

        @override
        async def generate_content_async(
            self, llm_request: LLMRequest, stream: bool = False
        ) -> AsyncGenerator[LLMResponse, None]:
            captured_b.append(llm_request)
            yield _chunk(" world")

    a = LLMAgent(name="a", llm=llm_a, include_contents="none")
    b = LLMAgent(name="b", llm=CaptureLLM(), include_contents="none")
    wf = Workflow(name="wf", edges=[Edge(START, a), Edge(a, b)])

    queue = _FakeEventQueue()
    ic = InvocationContext(event_queue=queue, task_id="t1", context_id="c1")
    ctx = Context(invocation_context=ic, node=wf)

    async for _ in wf.run(ctx):
        pass

    assert ctx.output == " world"
    assert len(queue.events) == 4
    assert cast(OpenAIChatCompletionUserMessage, captured_b[-1].messages[-1])["content"] == "hello"
