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
from new_choirworks.core.agents.llm_agent import LLMAgent
from new_choirworks.core.models.lite_llm import LiteLLM

from ..conftest import LLMKwargs


class _StreamingRequestHandler(Protocol):
    def on_message_send_stream(
        self, params: SendMessageRequest, context: ServerCallContext
    ) -> AsyncGenerator[Event, None]: ...


async def test_stream_text_response(llm_kwargs: LLMKwargs):
    """End-to-end: LLMAgent + BaseAgentExecutor + DefaultRequestHandler + InMemoryTaskStore
    → on_message_send_stream → verify event flow."""
    llm = LiteLLM(**llm_kwargs)
    agent = LLMAgent(name="test", llm=llm, instruction="You are a helpful assistant.")
    executor = BaseAgentExecutor(agent)
    handler = DefaultRequestHandler(
        agent_executor=executor,
        task_store=InMemoryTaskStore(),
        agent_card=build_agent_card("http://localhost:8080"),
    )
    # The SDK decorators erase the concrete method signature during inference.
    request_handler = cast(_StreamingRequestHandler, handler)

    try:
        request = SendMessageRequest(
            message=new_text_message("Hello", role=Role.ROLE_USER),
        )
        gen = request_handler.on_message_send_stream(request, ServerCallContext())
        events = [e async for e in gen]

        # Has Task events (SUBMITTED → WORKING)
        tasks = [e for e in events if isinstance(e, Task)]
        assert len(tasks) == 1

        # Has artifact events with LLM response text
        artifacts = [e for e in events if isinstance(e, TaskArtifactUpdateEvent)]
        assert len(artifacts) > 0
        text = "".join(p.text for e in artifacts for p in e.artifact.parts if p.text)
        assert text

        # Final status is COMPLETED
        status_updates = [e for e in events if isinstance(e, TaskStatusUpdateEvent)]
        assert status_updates
        assert status_updates[-1].status.state == TaskState.TASK_STATE_COMPLETED
    finally:
        await handler.aclose()
