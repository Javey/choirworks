from pathlib import Path

import yaml
from dotenv import load_dotenv

from new_choirworks.core.models.lite_llm import LiteLLM
from new_choirworks.core.models.llm_request import LLMRequest
from new_choirworks.core.models.llm_response import LLMResponse
from litellm.types.llms.openai import ChatCompletionToolParam

load_dotenv()


def _llm_kwargs():
    cfg = yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))["llm"]
    return {"model": cfg["planner_model"], "base_url": cfg["api_base"]}


async def test_stream_yields_responses():
    llm = LiteLLM(**_llm_kwargs())
    req = LLMRequest(messages=[{"role": "user", "content": "Hello"}])
    results = [r async for r in llm.generate_content_async(req, stream=True)]

    assert len(results) > 0
    assert all(isinstance(r, LLMResponse) for r in results)

    text = "".join(
        part.text for r in results if r.content for part in r.content.parts if not part.thought
    )
    assert text

    reasoning = "".join(
        part.text for r in results if r.content for part in r.content.parts if part.thought
    )
    assert reasoning


async def test_non_stream_yields_single_response():
    llm = LiteLLM(**_llm_kwargs())
    req = LLMRequest(messages=[{"role": "user", "content": "Hello"}])
    results = [r async for r in llm.generate_content_async(req, stream=False)]
    print(results)

    assert len(results) == 1
    parts = results[0].content.parts
    assert len(parts) == 2
    assert parts[0].thought is True
    assert parts[1].thought is False


async def test_stream_tool_call():
    llm = LiteLLM(**_llm_kwargs())
    req = LLMRequest(
        messages=[{"role": "user", "content": "北京的天气怎么样？"}],
        tools=[weather_tool],
    )
    results = [r async for r in llm.generate_content_async(req, stream=True)]

    function_calls = [fc for r in results for fc in r.get_function_calls()]
    assert len(function_calls) == 1
    assert function_calls[0].name == "get_weather"
    assert function_calls[0].args.get("city") is not None
    assert function_calls[0].id is not None


async def test_non_stream_tool_call():
    llm = LiteLLM(**_llm_kwargs())
    req = LLMRequest(
        messages=[{"role": "user", "content": "北京的天气怎么样？"}],
        tools=[weather_tool],
    )
    results = [r async for r in llm.generate_content_async(req, stream=False)]

    assert len(results) == 2
    function_calls = results[1].get_function_calls()
    assert len(function_calls) == 1
    assert function_calls[0].name == "get_weather"
    assert function_calls[0].args.get("city") is not None
    assert function_calls[0].id is not None


weather_tool: ChatCompletionToolParam = {
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get the current weather for a given location.",
        "parameters": {
            "type": "object",
            "properties": {
                "city": {"type": "string", "description": "The city to get the weather for."},
            },
            "required": ["city"]
        },
    },
}
