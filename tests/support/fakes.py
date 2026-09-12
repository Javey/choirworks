from __future__ import annotations

from typing import Any, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class FakeLLM:
    def __init__(
        self,
        structured_results: list[Any] | None = None,
        text_results: list[str] | None = None,
    ):
        self.structured_results = list(structured_results or [])
        self.text_results = list(text_results or [])
        self.structured_calls: list[dict[str, Any]] = []
        self.text_calls: list[dict[str, Any]] = []

    async def structured(self, *, system: str, user: str, schema: type[T]) -> T:
        self.structured_calls.append({"system": system, "user": user, "schema": schema})
        if not self.structured_results:
            raise AssertionError("FakeLLM has no scripted structured result")
        result = self.structured_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    async def text(self, *, system: str, user: str) -> str:
        self.text_calls.append({"system": system, "user": user})
        if not self.text_results:
            raise AssertionError("FakeLLM has no scripted text result")
        return self.text_results.pop(0)
