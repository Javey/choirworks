import json
from collections.abc import AsyncGenerator
from typing import cast, override

from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent
from google.protobuf import json_format
from litellm.types.llms.openai import ChatCompletionToolParam

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.agents.llm_agent import LLMAgent
from new_choirworks.core.events.event import to_a2a_artifact
from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.models.lite_llm import LiteLLM
from new_choirworks.core.models.llm_request import LLMRequest
from new_choirworks.core.models.llm_response import (
    FunctionCall,
    LLMResponse,
    LLMResponseContent,
    LLMResponsePart,
)
from new_choirworks.core.tools.base_tool import BaseTool

from ..conftest import FakeLLM, FakeTool, LLMKwargs


def _chunk(text: str, thought: bool = False) -> LLMResponse:
    return LLMResponse(
        content=LLMResponseContent(parts=[LLMResponsePart(text=text, thought=thought)])
    )


def _fc_chunk(name: str = "fake_tool", args: str = "{}", id: str = "call_1") -> LLMResponse:
    return LLMResponse(
        content=LLMResponseContent(
            parts=[
                LLMResponsePart(
                    function_call=FunctionCall(
                        id=id, name=name, args=cast(dict[str, object], json.loads(args))
                    )
                )
            ]
        )
    )


def _error_fc_chunk(
    name: str = "", id: str = "call_1", error_msg: str = "missing function name"
) -> LLMResponse:
    return LLMResponse(
        content=LLMResponseContent(
            parts=[LLMResponsePart(function_call=FunctionCall(id=id, name=name, args={}))]
        ),
        error_code="MALFORMED_FUNCTION_CALL",
        error_message=error_msg,
    )


def _ctx() -> InvocationContext:
    return InvocationContext(task_id="task-1", context_id="ctx-1")


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
    event = to_a2a_artifact(_ctx(), response, artifact_id="aid", append=True, last_chunk=False)
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
    event = to_a2a_artifact(_ctx(), response, artifact_id="aid", append=False, last_chunk=True)
    assert event.artifact.parts[0].metadata["thought"] == "true"
    assert "thought" not in event.artifact.parts[1].metadata
    assert event.append is False
    assert event.last_chunk is True


# --- _close ---


def test_close_merges_thought_and_text():
    agent = LLMAgent(name="t", llm=FakeLLM(responder=lambda i, r: []))
    result = agent.close(["reasoning"], ["answer"], [])
    assert result is not None
    assert result.content is not None
    parts = result.content.parts
    assert parts[0].text == "reasoning"
    assert parts[0].thought is True
    assert parts[1].text == "answer"
    assert parts[1].thought is False


def test_close_returns_none_for_empty_buffers():
    agent = LLMAgent(name="t", llm=FakeLLM(responder=lambda i, r: []))
    result = agent.close([], [], [])
    assert result is None


# --- append / last_chunk semantics ---


async def test_append_last_chunk_semantics():
    llm = FakeLLM(responder=lambda i, r: [_chunk("a"), _chunk("b"), _chunk("c")])
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
    llm = FakeLLM(responder=lambda i, r: [_chunk("你"), _chunk("好"), _chunk("！")])
    agent = LLMAgent(name="t", llm=llm)
    events = [e async for e in agent.run_async(_ctx())]
    artifacts = [e for e in events if isinstance(e, TaskArtifactUpdateEvent)]

    seal = artifacts[-1]
    assert seal.last_chunk is True
    assert len(seal.artifact.parts) == 1
    assert seal.artifact.parts[0].text == "你好！"


# --- thought metadata ---


async def test_thought_metadata():
    llm = FakeLLM(responder=lambda i, r: [_chunk("thinking...", thought=True), _chunk("answer")])
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

        @override
        async def generate_content_async(
            self, llm_request: LLMRequest, stream: bool = False
        ) -> AsyncGenerator[LLMResponse, None]:
            captured.append(llm_request)
            return
            yield  # pyright: ignore[reportUnreachable]

    agent = LLMAgent(name="t", llm=CaptureLLM(), instruction="You are helpful.")
    ctx = InvocationContext(messages=[{"role": "user", "content": "Hi"}])
    _ = [e async for e in agent.run_async(ctx)]

    req = captured[0]
    assert req.system_instruction == "You are helpful."
    assert req.messages[0]["role"] == "user"


# --- tool call flow (unit) ---


async def test_tool_call_flow():
    """Multi-turn: function_call → tool execute → text response."""
    llm = FakeLLM(
        responder=lambda i, r: (
            [_chunk("let me check"), _fc_chunk()] if i == 0 else [_chunk("北京天气晴")]
        )
    )
    tool = FakeTool(name="fake_tool", result={"weather": "sunny"})
    agent = LLMAgent(name="t", llm=llm, tools=[tool])
    ctx = InvocationContext(messages=[{"role": "user", "content": "北京天气"}])

    events = [e async for e in agent.run_async(ctx)]
    artifacts = [e for e in events if isinstance(e, TaskArtifactUpdateEvent)]

    # round 1: 1 text chunk + 1 fc chunk + 1 seal + 1 function_response
    # round 2: 1 chunk + 1 seal
    assert len(artifacts) == 6

    # round 1 seal has function_call
    seal1 = artifacts[2]
    assert seal1.last_chunk is True
    assert any(
        "type" in p.metadata and p.metadata["type"] == "function_call" for p in seal1.artifact.parts
    )

    # function_response
    fr = artifacts[3]
    assert any(
        "type" in p.metadata and p.metadata["type"] == "function_response"
        for p in fr.artifact.parts
    )

    # round 2 seal has only text
    seal2 = artifacts[5]
    assert seal2.last_chunk is True
    assert all("type" not in p.metadata for p in seal2.artifact.parts)

    # ctx.messages: user, assistant(tool_calls), tool, assistant(text)
    assert len(ctx.messages) == 4
    assert ctx.messages[0]["role"] == "user"
    assert ctx.messages[1]["role"] == "assistant"
    assert "tool_calls" in ctx.messages[1]
    assert ctx.messages[1]["tool_calls"] is not None
    assert ctx.messages[2]["role"] == "tool"
    assert ctx.messages[3]["role"] == "assistant"
    assert "tool_calls" in ctx.messages[3]
    assert ctx.messages[3]["tool_calls"] is None


async def test_error_retry():
    """3 rounds of malformed function_call, each yields seal + error response."""
    llm = FakeLLM(responder=lambda i, r: [_error_fc_chunk()])
    agent = LLMAgent(name="t", llm=llm, max_retries=3)
    ctx = InvocationContext(messages=[{"role": "user", "content": "hi"}])

    events = [e async for e in agent.run_async(ctx)]
    artifacts = [e for e in events if isinstance(e, TaskArtifactUpdateEvent)]

    # 3 rounds × (1 fc chunk + 1 seal + 1 error response) = 9
    assert len(artifacts) == 9

    for i in range(3):
        seal = artifacts[i * 3 + 1]
        err = artifacts[i * 3 + 2]
        assert seal.last_chunk is True
        assert err.last_chunk is True
        assert any(
            "type" in p.metadata and p.metadata["type"] == "function_call"
            for p in seal.artifact.parts
        )
        assert any(
            "type" in p.metadata and p.metadata["type"] == "function_response"
            for p in err.artifact.parts
        )

    # ctx.messages: user + 3×(assistant + tool) = 7
    assert len(ctx.messages) == 7


async def test_tool_result_normalized():
    """Non-dict result wrapped as {"result": value}."""
    llm = FakeLLM(responder=lambda i, r: [_fc_chunk()] if i == 0 else [_chunk("done")])
    tool = FakeTool(name="fake_tool", result="hello")
    agent = LLMAgent(name="t", llm=llm, tools=[tool])
    ctx = InvocationContext(messages=[{"role": "user", "content": "go"}])

    events = [e async for e in agent.run_async(ctx)]
    artifacts = [e for e in events if isinstance(e, TaskArtifactUpdateEvent)]

    fr = artifacts[2]
    fr_data = json_format.MessageToDict(fr.artifact.parts[0].data)
    assert fr_data["response"] == {"result": "hello"}


async def test_function_call_id_consistency():
    """function_call id in seal matches function_response id."""
    llm = FakeLLM(responder=lambda i, r: [_fc_chunk(id="call_42")] if i == 0 else [_chunk("done")])
    tool = FakeTool(name="fake_tool", result={"ok": True})
    agent = LLMAgent(name="t", llm=llm, tools=[tool])
    ctx = InvocationContext(messages=[{"role": "user", "content": "go"}])

    events = [e async for e in agent.run_async(ctx)]
    artifacts = [e for e in events if isinstance(e, TaskArtifactUpdateEvent)]

    seal = artifacts[1]
    fr = artifacts[2]

    fc_id = cast(str, json_format.MessageToDict(seal.artifact.parts[0].data)["id"])
    fr_id = cast(str, json_format.MessageToDict(fr.artifact.parts[0].data)["id"])
    assert fc_id == fr_id == "call_42"


# --- integration (real LLM) ---


async def test_text_response(llm_kwargs: LLMKwargs):
    llm = LiteLLM(**llm_kwargs)
    agent = LLMAgent(name="test", llm=llm)
    ctx = InvocationContext(messages=[{"role": "user", "content": "Hello"}])

    results = [e async for e in agent.run_async(ctx)]

    assert len(results) > 0
    artifacts = [e for e in results if isinstance(e, TaskArtifactUpdateEvent)]
    assert len(artifacts) > 0
    assert artifacts[-1].last_chunk is True
    text = "".join(p.text for e in artifacts for p in e.artifact.parts if p.text)
    assert text


# --- integration: tool call (real LLM) ---


class WeatherTool(BaseTool):
    """A simple weather tool for integration testing."""

    name: str = "get_weather"
    description: str = "Get the current weather for a given city."

    @override
    def get_declaration(self) -> ChatCompletionToolParam:
        return {
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get the current weather for a given city.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "city": {
                            "type": "string",
                            "description": "The city to get the weather for.",
                        },
                    },
                    "required": ["city"],
                },
            },
        }

    @override
    async def run_async(self, args: dict[str, object]) -> object:
        city = args.get("city", "")
        return {"city": city, "weather": "sunny", "temp": 25}


async def test_tool_call_integration(llm_kwargs: LLMKwargs):
    llm = LiteLLM(**llm_kwargs)
    agent = LLMAgent(name="test", llm=llm, tools=[WeatherTool()])
    ctx = InvocationContext(messages=[{"role": "user", "content": "北京的天气怎么样？"}])

    results = [e async for e in agent.run_async(ctx)]

    # print(results)

    assert len(results) > 0
    artifacts = [e for e in results if isinstance(e, TaskArtifactUpdateEvent)]

    has_function_call = any(
        any(
            "type" in p.metadata and p.metadata["type"] == "function_call" for p in e.artifact.parts
        )
        for e in artifacts
    )
    assert has_function_call

    has_function_response = any(
        any(
            "type" in p.metadata and p.metadata["type"] == "function_response"
            for p in e.artifact.parts
        )
        for e in artifacts
    )
    assert has_function_response

    fc_ids = [
        json_format.MessageToDict(p.data)["id"]
        for e in artifacts
        for p in e.artifact.parts
        if "type" in p.metadata and p.metadata["type"] == "function_call"
    ]
    fr_ids = [
        json_format.MessageToDict(p.data)["id"]
        for e in artifacts
        for p in e.artifact.parts
        if "type" in p.metadata and p.metadata["type"] == "function_response"
    ]
    assert fc_ids
    assert fr_ids
    assert set(fc_ids) == set(fr_ids)

    assert artifacts[-1].last_chunk is True
    text = "".join(
        p.text for e in artifacts for p in e.artifact.parts if p.text and "type" not in p.metadata
    )
    assert text

    assert len(ctx.messages) > 1
