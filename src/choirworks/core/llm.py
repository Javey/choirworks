from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, TypeVar

import litellm
from litellm.litellm_core_utils.streaming_handler import CustomStreamWrapper
from litellm.types.utils import ChatCompletionDeltaToolCall, Delta, ModelResponse
from pydantic import BaseModel

if TYPE_CHECKING:
    from choirworks.tools.base import AgentFunction, FunctionContext, ToolCallResult

T = TypeVar("T", bound=BaseModel)

type CompletionResult = ModelResponse | CustomStreamWrapper
type CompletionFn = Callable[..., Awaitable[CompletionResult]]


class LiteLLMClient:
    """LLM client backed by litellm.

    ``stream`` is the core method: it sends messages (optionally with tool
    declarations) to the model and streams back ``Delta`` chunks followed by
    a ``ToolCallResult`` when the model invokes a tool.  The client does NOT
    execute tools — that is the caller's responsibility.

    ``text`` is the plain non-streaming completion used for summaries.
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

    async def stream(
        self,
        *,
        system: str,
        user: str,
        tools: list[AgentFunction] | None = None,
        ctx: FunctionContext | None = None,
        tool_choice: str | dict[str, object] = "auto",
    ) -> AsyncIterator[Delta | ToolCallResult]:
        """Stream ``Delta`` chunks, then yield a ``ToolCallResult`` if the
        model invokes a tool.

        When *tools* and *ctx* are provided, each tool's ``args_model(ctx)``
        is awaited to build the function-tool declarations sent to the model.
        Tool-call argument fragments are accumulated across chunks and
        validated with the tool's schema once the stream ends.

        The client does NOT execute the tool — it yields a
        :class:`ToolCallResult` for the caller to act on.
        """
        from choirworks.tools.base import ToolCallResult  # runtime import

        declarations: list[dict[str, object]] = []
        schemas: dict[str, type[BaseModel]] = {}
        functions: dict[str, AgentFunction] = {}
        if tools and ctx:
            for tool in tools:
                schema = await tool.args_model(ctx)
                schemas[tool.name] = schema
                functions[tool.name] = tool
                declarations.append({
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": schema.model_json_schema(),
                    },
                })

        response = await self._completion_fn(
            model=self._model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            timeout=self._timeout,
            stream=True,
            tools=declarations or None,
            tool_choice=tool_choice if declarations else None,
            **self._extra_kwargs(),
        )
        if not isinstance(response, CustomStreamWrapper):
            raise ValueError("expected a streaming response")

        fragments: dict[int, list[str]] = {}
        call_names: dict[int, str] = {}
        async for chunk in response:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta
            if delta is None:
                continue
            yield delta
            for call in delta.tool_calls or []:
                if isinstance(call, ChatCompletionDeltaToolCall):
                    fragments.setdefault(call.index, []).append(
                        call.function.arguments
                    )
                    if call.function.name:
                        call_names[call.index] = call.function.name

        if not fragments:
            return

        if len(fragments) > 1:
            raise ValueError("expected a single tool call")

        index, args_fragments = next(iter(fragments.items()))
        tool_name = call_names.get(index, "")
        if not tool_name or tool_name not in functions:
            raise ValueError(f"model called unknown tool: {tool_name}")

        payload = "".join(args_fragments)
        schema = schemas[tool_name]
        args = schema.model_validate_json(payload)
        yield ToolCallResult(function=functions[tool_name], args=args)

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
