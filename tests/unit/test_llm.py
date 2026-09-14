from pydantic import BaseModel

from choirworks.core.llm import LiteLLMClient


class Answer(BaseModel):
    value: str


async def test_structured_uses_injected_completion_fn():
    import json

    from litellm.types.utils import (
        ChatCompletionMessageToolCall,
        Choices,
        Message,
        ModelResponse,
    )

    calls = []

    async def fake_completion(**kwargs):
        calls.append(kwargs)
        tool_call = ChatCompletionMessageToolCall(
            id="call_1",
            type="function",
            function={
                "name": "Answer",
                "arguments": json.dumps({"value": "ok"}),
            },
        )
        msg = Message(content="reasoning", role="assistant", tool_calls=[tool_call])
        return ModelResponse(
            id="test", created=0, model="test",
            choices=[Choices(finish_reason="tool_calls", index=0, message=msg)],
            object="chat.completion",
        )

    client = LiteLLMClient(
        model="openai/test-model",
        timeout_seconds=5.0,
        completion_fn=fake_completion,
    )
    result = await client.structured(system="sys", user="usr", schema=Answer)
    assert result.value == "ok"
    assert calls[0]["model"] == "openai/test-model"
    assert calls[0]["timeout"] == 5.0
    assert calls[0]["messages"][0]["content"] == "sys"
    assert "tools" in calls[0]
    assert calls[0]["tools"][0]["function"]["name"] == "Answer"


async def test_text_uses_injected_completion_fn():
    from litellm.types.utils import Choices, Message, ModelResponse

    async def fake_completion(**kwargs):
        msg = Message(content="hello world", role="assistant")
        return ModelResponse(
            id="test", created=0, model="test",
            choices=[Choices(finish_reason="stop", index=0, message=msg)],
            object="chat.completion",
        )

    client = LiteLLMClient(model="test", completion_fn=fake_completion)
    result = await client.text(system="sys", user="usr")
    assert result == "hello world"
