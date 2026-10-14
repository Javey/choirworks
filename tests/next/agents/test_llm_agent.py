from collections.abc import AsyncGenerator

from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.agents.llm_agent import LLMAgent
from new_choirworks.core.events.event import to_a2a_artifact
from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.models.lite_llm import LiteLLM
from new_choirworks.core.models.llm_request import LLMRequest
from new_choirworks.core.models.llm_response import (
    LLMResponse,
    LLMResponseContent,
    LLMResponsePart,
)


class FakeLLM(BaseLLM):
    """A fake LLM that yields predefined responses for deterministic testing."""

    model: str = "fake"
    responses: list[LLMResponse]

    async def generate_content_async(
        self, llm_request: LLMRequest, stream: bool = False
    ) -> AsyncGenerator[LLMResponse, None]:
        for r in self.responses:
            yield r


def _chunk(text: str, thought: bool = False) -> LLMResponse:
    return LLMResponse(
        content=LLMResponseContent(
            parts=[LLMResponsePart(text=text, thought=thought)]
        )
    )


def _ctx() -> InvocationContext:
    return InvocationContext(
        task_id="task-1", context_id="ctx-1"
    )


# --- to_a2a_artifact ---


def test_to_a2a_artifact_skips_none_text():
    response = LLMResponse(
        content=LLMResponseContent(
            parts=[
                LLMResponsePart(text=None),
                LLMResponsePart(text="hello"),
            ]
        )
    )
    event = to_a2a_artifact(
        _ctx(), response, artifact_id="aid", append=True, last_chunk=False
    )
    assert event.artifact.parts[0].text == "hello"
    assert len(event.artifact.parts) == 1


def test_to_a2a_artifact_marks_thought():
    response = LLMResponse(
        content=LLMResponseContent(
            parts=[
                LLMResponsePart(text="thinking", thought=True),
                LLMResponsePart(text="answer", thought=False),
            ]
        )
    )
    event = to_a2a_artifact(
        _ctx(), response, artifact_id="aid", append=False, last_chunk=True
    )
    assert event.artifact.parts[0].metadata["thought"] == "true"
    assert "thought" not in event.artifact.parts[1].metadata
    assert event.append is False
    assert event.last_chunk is True


# --- _close ---


def test_close_merges_thought_and_text():
    agent = LLMAgent(name="t", llm=FakeLLM(responses=[]))
    result = agent._close(["reasoning"], ["answer"])
    assert result is not None
    assert result.content is not None
    parts = result.content.parts
    assert parts[0].text == "reasoning"
    assert parts[0].thought is True
    assert parts[1].text == "answer"
    assert parts[1].thought is False


def test_close_returns_none_for_empty_buffers():
    agent = LLMAgent(name="t", llm=FakeLLM(responses=[]))
    result = agent._close([], [])
    assert result is None


# --- append / last_chunk semantics ---


async def test_append_last_chunk_semantics():
    llm = FakeLLM(responses=[_chunk("a"), _chunk("b"), _chunk("c")])
    agent = LLMAgent(name="t", llm=llm)
    events = [e async for e in agent.run_async(_ctx())]
    artifacts = [e for e in events if isinstance(e, TaskArtifactUpdateEvent)]

    # 3 chunks + 1 seal = 4 events
    assert len(artifacts) == 4
    assert artifacts[0].append is False
    assert artifacts[0].last_chunk is False
    assert artifacts[1].append is True
    assert artifacts[1].last_chunk is False
    assert artifacts[2].append is True
    assert artifacts[2].last_chunk is False
    assert artifacts[3].append is False
    assert artifacts[3].last_chunk is True


# --- seal content ---


async def test_seal_contains_full_text():
    llm = FakeLLM(responses=[_chunk("你"), _chunk("好"), _chunk("！")])
    agent = LLMAgent(name="t", llm=llm)
    events = [e async for e in agent.run_async(_ctx())]
    artifacts = [e for e in events if isinstance(e, TaskArtifactUpdateEvent)]

    seal = artifacts[-1]
    assert seal.last_chunk is True
    assert len(seal.artifact.parts) == 1
    assert seal.artifact.parts[0].text == "你好！"


# --- thought metadata ---


async def test_thought_metadata():
    llm = FakeLLM(
        responses=[
            _chunk("thinking...", thought=True),
            _chunk("answer", thought=False),
        ]
    )
    agent = LLMAgent(name="t", llm=llm)
    events = [e async for e in agent.run_async(_ctx())]
    artifacts = [e for e in events if isinstance(e, TaskArtifactUpdateEvent)]

    # chunk 0: thought only
    assert artifacts[0].artifact.parts[0].metadata["thought"] == "true"
    # chunk 1: text only
    assert "thought" not in artifacts[1].artifact.parts[0].metadata
    # seal: both thought + text, thought part has metadata
    seal = artifacts[-1]
    assert seal.artifact.parts[0].metadata["thought"] == "true"
    assert "thought" not in seal.artifact.parts[1].metadata


# --- instruction ---


async def test_instruction_added_as_system_message():
    captured: list[LLMRequest] = []

    class CaptureLLM(BaseLLM):
        model: str = "fake"

        async def generate_content_async(
            self, llm_request: LLMRequest, stream: bool = False
        ) -> AsyncGenerator[LLMResponse, None]:
            captured.append(llm_request)
            return
            yield  # make it a generator

    agent = LLMAgent(
        name="t", llm=CaptureLLM(), instruction="You are helpful."
    )
    ctx = InvocationContext(messages=[{"role": "user", "content": "Hi"}])
    _ = [e async for e in agent.run_async(ctx)]

    req = captured[0]
    assert req.messages[0]["role"] == "system"
    assert req.messages[0]["content"] == "You are helpful."
    assert req.messages[1]["role"] == "user"


# --- integration (real LLM) ---


async def test_text_response(llm_kwargs):
    llm = LiteLLM(**llm_kwargs)
    agent = LLMAgent(name="test", llm=llm)
    ctx = InvocationContext(messages=[{"role": "user", "content": "Hello"}])

    results = [e async for e in agent.run_async(ctx)]

    assert len(results) > 0
    artifacts = [e for e in results if isinstance(e, TaskArtifactUpdateEvent)]
    assert len(artifacts) > 0
    assert artifacts[-1].last_chunk is True
    text = "".join(
        p.text for e in artifacts for p in e.artifact.parts if p.text
    )
    assert text
