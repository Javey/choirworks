from __future__ import annotations

from typing import Protocol, TypeVar

import instructor
import litellm
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class LLMClient(Protocol):
    async def structured(self, *, system: str, user: str, schema: type[T]) -> T: ...

    async def text(self, *, system: str, user: str) -> str: ...


class LiteLLMClient:
    def __init__(self, model: str, timeout_seconds: float = 60.0):
        self._model = model
        self._timeout = timeout_seconds
        self._instructor = instructor.from_litellm(litellm.acompletion)

    async def structured(self, *, system: str, user: str, schema: type[T]) -> T:
        return await self._instructor.chat.completions.create(
            model=self._model,
            response_model=schema,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            timeout=self._timeout,
        )

    async def text(self, *, system: str, user: str) -> str:
        response = await litellm.acompletion(
            model=self._model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            timeout=self._timeout,
        )
        return response.choices[0].message.content or ""
