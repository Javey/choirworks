from __future__ import annotations

import json
from typing import Any, TypeVar

from litellm.types.utils import (
    ChatCompletionMessageToolCall,
    Choices,
    Message,
    ModelResponse,
)
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class FakeLLM:
    """Mock LLM for testing.

    Acts as both:
    1. A ``completion_fn`` (callable) for ``LiteLLMClient(completion_fn=...)``
    2. A direct LLM client with ``structured()`` / ``text()`` methods
       for use with ``Planner`` / ``Orchestrator`` that accept a client directly.

    Pass ``structured_results`` (list of Pydantic models) to script
    ``structured()`` calls in order.
    """

    def __init__(
        self,
        structured_results: list[Any] | None = None,
        text_results: list[str] | None = None,
    ):
        self.structured_results = list(structured_results or [])
        self.text_results = list(text_results or [])
        self.structured_calls: list[dict[str, Any]] = []
        self.text_calls: list[dict[str, Any]] = []
        self._call_id = 0

    async def __call__(self, **kwargs: Any) -> ModelResponse:
        """completion_fn interface — used by LiteLLMClient."""
        tools = kwargs.get("tools")
        if tools:
            self.structured_calls.append(kwargs)
            if not self.structured_results:
                raise AssertionError("FakeLLM has no scripted structured result")
            result = self.structured_results.pop(0)
            if isinstance(result, Exception):
                raise result

            tool_name = tools[0]["function"]["name"]
            self._call_id += 1
            tool_call = ChatCompletionMessageToolCall(
                id=f"call_{self._call_id}",
                type="function",
                function={
                    "name": tool_name,
                    "arguments": json.dumps(result.model_dump(), ensure_ascii=False),
                },
            )
            msg = Message(content="", role="assistant", tool_calls=[tool_call])
            return ModelResponse(
                id=f"fake_{self._call_id}",
                created=0,
                model="fake",
                choices=[Choices(finish_reason="tool_calls", index=0, message=msg)],
                object="chat.completion",
            )
        else:
            self.text_calls.append(kwargs)
            if not self.text_results:
                raise AssertionError("FakeLLM has no scripted text result")
            text = self.text_results.pop(0)
            msg = Message(content=text, role="assistant")
            return ModelResponse(
                id="fake_text",
                created=0,
                model="fake",
                choices=[Choices(finish_reason="stop", index=0, message=msg)],
                object="chat.completion",
            )

    async def structured(self, *, system: str, user: str, schema: type[T]) -> T:
        """Direct LLM client interface — used by Planner/Orchestrator."""
        self.structured_calls.append({"system": system, "user": user, "schema": schema})
        if not self.structured_results:
            raise AssertionError("FakeLLM has no scripted structured result")
        result = self.structured_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def text(self, *, system: str, user: str) -> str:
        """Direct LLM client interface — used by Planner/Orchestrator."""
        self.text_calls.append({"system": system, "user": user})
        if not self.text_results:
            raise AssertionError("FakeLLM has no scripted text result")
        return self.text_results.pop(0)

    def count_tokens(self, text: str) -> int:
        return len(text) // 3

    def get_context_window(self) -> int:
        return 128000
