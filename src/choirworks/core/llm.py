from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeVar

import litellm
from litellm.litellm_core_utils.streaming_handler import CustomStreamWrapper
from litellm.types.utils import ChatCompletionDeltaToolCall, ModelResponse
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

type CompletionResult = ModelResponse | CustomStreamWrapper
type CompletionFn = Callable[..., Awaitable[CompletionResult]]


class LiteLLMClient:
    """LLM client backed by litellm.

    ``stream_structured`` enforces the schema with a forced function tool and
    streams thinking deltas followed by the validated result.  ``text`` is the
    plain non-streaming completion used for summaries.
    """

    def __init__(
        self,
        model: str,
        timeout_seconds: float = 60.0,
        *,
        api_base: str | None = None,
        context_window: int | None = None,
        completion_fn: CompletionFn | None = None,
    ):
        self._model = model
        self._timeout = timeout_seconds
        self._api_base = api_base
        self._context_window = context_window
        self._completion_fn: CompletionFn = completion_fn or litellm.acompletion

    def _extra_kwargs(self) -> dict[str, str]:
        if self._api_base:
            return {"api_base": self._api_base}
        return {}

    async def stream_structured(
        self,
        *,
        system: str,
        user: str,
        schema: type[T],
        tool_name: str | None = None,
    ) -> AsyncIterator[str | T]:
        """Stream thinking deltas, then yield the validated tool-call result.

        The schema is enforced at the API layer via a forced function tool.
        Argument fragments are accumulated across chunks and validated with
        pydantic once the stream ends.
        """
        name = tool_name or schema.__name__
        tool = {
            "type": "function",
            "function": {
                "name": name,
                "parameters": schema.model_json_schema(),
            },
        }
        response = await self._completion_fn(
            model=self._model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            timeout=self._timeout,
            stream=True,
            tools=[tool],
            tool_choice={"type": "function", "function": {"name": name}},
            **self._extra_kwargs(),
        )
        if not isinstance(response, CustomStreamWrapper):
            raise ValueError("expected a streaming response")

        fragments: dict[int, list[str]] = {}
        async for chunk in response:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta
            if delta is None:
                continue
            reasoning = (
                delta.reasoning_content
                if hasattr(delta, "reasoning_content")
                else None
            )
            text = reasoning or delta.content
            if text:
                yield text
            for call in delta.tool_calls or []:
                if isinstance(call, ChatCompletionDeltaToolCall):
                    fragments.setdefault(call.index, []).append(
                        call.function.arguments
                    )

        if not fragments:
            raise ValueError(f"model did not call the {name} tool")
        if len(fragments) > 1:
            raise ValueError(f"expected a single {name} tool call")
        payload = "".join(next(iter(fragments.values())))
        yield schema.model_validate_json(payload)

    async def text(self, *, system: str, user: str) -> str:
        response = await self._completion_fn(
            model=self._model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            timeout=self._timeout,
            **self._extra_kwargs(),
        )
        if not isinstance(response, ModelResponse):
            raise ValueError("expected a non-streaming response")
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
            return int(info.get("max_input_tokens") or 128000)
        except Exception:  # noqa: BLE001
            return 128000
