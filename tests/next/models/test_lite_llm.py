from pathlib import Path

import yaml
from dotenv import load_dotenv

from new_choirworks.core.models.lite_llm import LiteLLM
from new_choirworks.core.models.llm_request import LLMRequest
from new_choirworks.core.models.llm_response import LLMResponse

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
