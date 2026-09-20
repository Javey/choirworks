from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, TypeVar

from litellm.types.utils import Delta
from pydantic import BaseModel

from choirworks.core.planner import PlanDraft

T = TypeVar("T", bound=BaseModel)


class FakeLLM:
    """Mock LLM client for testing Planner / ContextBriefBuilder.

    Pass ``structured_results`` (list of Pydantic models) to script
    ``stream_structured()`` calls in order, and ``text_results`` to script
    ``text()`` calls.
    """

    def __init__(
        self,
        structured_results: list[Any] | None = None,
        text_results: list[str] | None = None,
    ):
        self.structured_results = list(structured_results or [])
        self.text_results = list(text_results or [])
        self.stream_calls: list[dict[str, Any]] = []
        self.text_calls: list[dict[str, Any]] = []

    async def stream_structured(
        self, *, system: str, user: str, schema: type[T], tool_name: str | None = None
    ) -> AsyncIterator[Any]:
        """Direct LLM client interface — streams reasoning deltas then the result."""
        self.stream_calls.append(
            {
                "system": system,
                "user": user,
                "schema": schema,
                "tool_name": tool_name,
            }
        )
        if not self.structured_results:
            raise AssertionError("FakeLLM has no scripted structured result")
        result = self.structured_results.pop(0)
        if isinstance(result, Exception):
            raise result
        reasoning = (
            f"思考：将请求拆解为 {len(result.nodes)} 个节点。"
            if isinstance(result, PlanDraft)
            else "思考：解读产出。"
        )
        midpoint = len(reasoning) // 2
        yield Delta(reasoning_content=reasoning[:midpoint])
        yield Delta(reasoning_content=reasoning[midpoint:])
        yield result

    async def text(self, *, system: str, user: str) -> str:
        """Direct LLM client interface — used by ContextBriefBuilder."""
        self.text_calls.append({"system": system, "user": user})
        if not self.text_results:
            raise AssertionError("FakeLLM has no scripted text result")
        return self.text_results.pop(0)

    def count_tokens(self, text: str) -> int:
        return len(text) // 3

    def get_context_window(self) -> int:
        return 128000
