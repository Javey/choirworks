from types import SimpleNamespace

from pydantic import BaseModel

from choirworks.core.llm import LiteLLMClient


class Answer(BaseModel):
    value: str


async def test_structured_passes_schema_and_model(monkeypatch):
    calls = []

    async def fake_create(**kwargs):
        calls.append(kwargs)
        return Answer(value="ok")

    client = LiteLLMClient(model="openai/test-model", timeout_seconds=5.0)
    monkeypatch.setattr(
        client,
        "_instructor",
        SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=fake_create))),
    )
    result = await client.structured(system="sys", user="usr", schema=Answer)
    assert result == Answer(value="ok")
    assert calls[0]["model"] == "openai/test-model"
    assert calls[0]["response_model"] is Answer
    assert calls[0]["timeout"] == 5.0
    assert calls[0]["messages"][0]["content"] == "sys"
