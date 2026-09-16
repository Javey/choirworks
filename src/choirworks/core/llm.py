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
        api_base: str | None = None,
        context_window: int | None = None,
        completion_fn: Any | None = None,
    ):
        self._model = model
        self._timeout = timeout_seconds
        self._api_base = api_base
        self._context_window = context_window
        self._completion_fn = completion_fn or litellm.acompletion
        self._instructor = instructor.from_litellm(self._completion_fn)

    def _extra_kwargs(self) -> dict[str, Any]:
        if self._api_base:
            return {"api_base": self._api_base}
        return {}

    async def structured(self, *, system: str, user: str, schema: type[T]) -> T:
        return await self._instructor.chat.completions.create(
            model=self._model,
            response_model=schema,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            timeout=self._timeout,
            **self._extra_kwargs(),
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
            **self._extra_kwargs(),
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
            **self._extra_kwargs(),
        )
        return response.choices[0].message.content or ""

    def count_tokens(self, text: str) -> int:
        try:
            return litellm.token_counter(model=self._model, text=text)
        except Exception:  # noqa: BLE001
            return len(text) // 3

    def get_context_window(self) -> int:
        if self._context_window:
            return self._context_window
        try:
            info = litellm.get_model_info(self._model)
            return int(info.get("max_input_tokens", 128000))
        except Exception:  # noqa: BLE001
            return 128000
