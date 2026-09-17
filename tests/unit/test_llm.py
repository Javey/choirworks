from choirworks.core.llm import LiteLLMClient


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
