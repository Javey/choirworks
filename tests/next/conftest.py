from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import TypedDict, cast, override

import pytest
import yaml
from dotenv import load_dotenv
from litellm.types.llms.openai import ChatCompletionToolParam

from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.models.llm_request import LLMRequest
from new_choirworks.core.models.llm_response import LLMResponse
from new_choirworks.core.tools.base_tool import BaseTool

_ = load_dotenv()


class LLMKwargs(TypedDict):
    model: str
    base_url: str


@pytest.fixture
def llm_kwargs() -> LLMKwargs:
    cfg = cast(
        dict[str, dict[str, str]], yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))
    )
    llm_cfg = cfg["llm"]
    return {"model": llm_cfg["model"], "base_url": llm_cfg["api_base"]}


class FakeLLM(BaseLLM):
    """A fake LLM that yields responses via a callable for deterministic testing."""

    model: str = "fake"
    responder: Callable[[int, LLMRequest], list[LLMResponse]]
    call_count: int = 0

    @override
    async def generate_content_async(
        self, llm_request: LLMRequest, stream: bool = False
    ) -> AsyncGenerator[LLMResponse, None]:
        responses = self.responder(self.call_count, llm_request)
        self.call_count += 1
        for r in responses:
            yield r


class FakeTool(BaseTool):
    """A fake tool with a predefined result."""

    name: str = "fake_tool"
    description: str = "A fake tool for testing."
    result: object = None

    @override
    def get_declaration(self) -> ChatCompletionToolParam:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": {"type": "object", "properties": {}},
            },
        }

    @override
    async def run_async(self, args: dict[str, object]) -> object:
        return self.result
