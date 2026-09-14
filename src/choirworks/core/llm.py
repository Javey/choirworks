from __future__ import annotations

from typing import Any, TypeVar

import instructor
import litellm
from litellm.types.utils import ModelResponse
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class LiteLLMClient:
    """LLM client backed by litellm + instructor.

    Pass ``completion_fn`` to inject a mock ``litellm.acompletion``-compatible
    function for testing/offline simulation.  The mock must return a
    ``ModelResponse`` whose ``choices[0].message`` contains:

    * ``content``  – free-form reasoning / text
    * ``tool_calls`` – structured payload (when using ``structured()``)
    """

    def __init__(
        self,
        model: str,
        timeout_seconds: float = 60.0,
        *,
        completion_fn: Any | None = None,
    ):
        self._model = model
        self._timeout = timeout_seconds
        self._completion_fn = completion_fn or litellm.acompletion
        self._instructor = instructor.from_litellm(self._completion_fn)

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

    async def structured_with_raw(
        self, *, system: str, user: str, schema: type[T]
    ) -> tuple[T, str]:
        """Structured call that also returns the model's free-form reasoning text."""
        result, raw = await self._instructor.chat.completions.create_with_completion(
            model=self._model,
            response_model=schema,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            timeout=self._timeout,
        )
        content = ""
        try:
            content = raw.choices[0].message.content or ""
        except (AttributeError, IndexError, TypeError):
            content = ""
        return result, content

    async def text(self, *, system: str, user: str) -> str:
        response: ModelResponse = await self._completion_fn(
            model=self._model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            timeout=self._timeout,
        )
        return response.choices[0].message.content or ""
