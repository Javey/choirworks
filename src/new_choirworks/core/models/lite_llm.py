from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import cast, override

import litellm
from litellm.litellm_core_utils.streaming_handler import CustomStreamWrapper
from litellm.types.utils import Delta, Message, ModelResponse
import json

from .base_llm import BaseLLM
from .llm_request import LLMRequest
from .llm_response import FunctionCall, LLMResponse, LLMResponseContent, LLMResponsePart

type CompletionResult = ModelResponse | CustomStreamWrapper
type CompletionFn = Callable[..., Awaitable[CompletionResult]]


@dataclass
class _ToolCallBuffer:
    id: str
    name: str
    args: list[str] | None = field(default_factory=list)


class LiteLLM(BaseLLM):
    completion_fn: CompletionFn = cast(CompletionFn, litellm.acompletion)
    base_url: str | None = None

    @override
    async def generate_content_async(
        self, llm_request: LLMRequest, stream: bool = False
    ) -> AsyncGenerator[LLMResponse, None]:
        effective_model = llm_request.model or self.model
        kwargs = {
            "model": effective_model,
            "messages": llm_request.messages,
            "stream": stream,
            "base_url": self.base_url,
        }
        if llm_request.tools:
            kwargs["tools"] = llm_request.tools
        response = await self.completion_fn(**kwargs)

        if stream:
            if not isinstance(response, CustomStreamWrapper):
                raise ValueError("Expected a CustomStreamWrapper for streaming response.")
            tool_call_buffer: dict[int, _ToolCallBuffer] = {}
            async for chunk in response:
                print(chunk)
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta
                if delta.tool_calls:
                    for call in delta.tool_calls:
                        index = call.index
                        if index not in tool_call_buffer:
                            tool_call_buffer[index] = _ToolCallBuffer(
                                id=call.id,
                                name=call.function.name,
                                args=[],
                            )
                        if call.function.arguments:
                            tool_call_buffer[index].args.append(call.function.arguments)
                parts = _build_parts(delta)
                if parts:
                    yield LLMResponse(content=LLMResponseContent(parts=parts))
            if tool_call_buffer:
                for index, buffer in tool_call_buffer.items():
                    yield _build_function_call_response(
                        function_name=buffer.name,
                        function_args="".join(buffer.args),
                        call_id=buffer.id
                    )

        else:
            if not isinstance(response, ModelResponse):
                raise ValueError("Expected a ModelResponse for non-streaming response.")
            yield LLMResponse(
                content=LLMResponseContent(parts=_build_parts(response.choices[0].message))
            )


def _build_parts(message: Delta | Message) -> list[LLMResponsePart]:
    parts: list[LLMResponsePart] = []
    reasoning: str | None = getattr(message, "reasoning_content", None)
           
    if reasoning:
        parts.append(LLMResponsePart(text=reasoning, thought=True))
    if message.content:
        parts.append(LLMResponsePart(text=message.content, thought=False))
    return parts

def _build_function_call_response(
    function_name: str, function_args: str | None, call_id: str
) -> LLMResponse:
    """Yield a function response as an LLMResponse."""
    return LLMResponse(
        content=LLMResponseContent(
            parts=[
                LLMResponsePart(
                    thought=False,
                    function_call=FunctionCall(
                        id=call_id,
                        name=function_name,
                        args=json.loads(function_args) if function_args else None,
                    ),
                )
            ]
        )
    )