import json
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import cast, override

import litellm
from litellm.litellm_core_utils.streaming_handler import CustomStreamWrapper
from litellm.types.utils import (
    ChatCompletionDeltaToolCall,
    ChatCompletionMessageToolCall,
    Delta,
    Message,
    ModelResponse,
)

from .base_llm import BaseLLM
from .llm_request import LLMRequest
from .llm_response import FunctionCall, LLMResponse, LLMResponseContent, LLMResponsePart

type CompletionResult = ModelResponse | CustomStreamWrapper
type CompletionFn = Callable[..., Awaitable[CompletionResult]]

@dataclass
class _ToolCallBuffer:
    id: str
    name: str
    args: list[str] = field(default_factory=list)

class LiteLLM(BaseLLM):
    completion_fn: CompletionFn = cast(CompletionFn, litellm.acompletion)
    base_url: str | None = None

    @override
    async def generate_content_async(
        self, llm_request: LLMRequest, stream: bool = False
    ) -> AsyncGenerator[LLMResponse, None]:
        effective_model = llm_request.model or self.model
        kwargs: dict[str, object] = {
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
                        if not isinstance(call, ChatCompletionDeltaToolCall):
                            continue
                        index = call.index
                        if index not in tool_call_buffer:
                            tool_call_buffer[index] = _ToolCallBuffer(
                                id=call.id or "",
                                name=call.function.name or "",
                                args=[],
                            )
                        if call.id:
                            tool_call_buffer[index].id = call.id
                        if call.function.name:
                            tool_call_buffer[index].name = call.function.name
                        if call.function.arguments:
                            tool_call_buffer[index].args.append(call.function.arguments)
                parts = _build_parts(delta)
                if parts:
                    yield LLMResponse(content=LLMResponseContent(parts=parts))
            if tool_call_buffer:
                for buffer in tool_call_buffer.values():
                    yield _build_function_call_response(
                        function_name=buffer.name,
                        function_args="".join(buffer.args),
                        call_id=buffer.id
                    )

        else:
            if not isinstance(response, ModelResponse):
                raise ValueError("Expected a ModelResponse for non-streaming response.")
            message = response.choices[0].message
            parts=_build_parts(message)
            if parts:
                yield LLMResponse(
                    content=LLMResponseContent(parts=parts)
                )
            if message.tool_calls:
                for call in message.tool_calls:
                    if not isinstance(call, ChatCompletionMessageToolCall):
                        continue
                    yield _build_function_call_response(
                        function_name=call.function.name,
                        function_args=call.function.arguments,
                        call_id=call.id,
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
    function_name: str | None, function_args: str | None, call_id: str | None
) -> LLMResponse:
    """构造 FunctionCall 的 LLMResponse，解析失败时保留原始信息并设错误字段。"""
    error_code: str | None = None
    error_message: str | None = None
    args: dict[str, object] = {}

    if not function_name:
        error_code = "MALFORMED_FUNCTION_CALL"
        error_message = f"missing function name, call_id={call_id}"
    else:
        try:
            args = json.loads(function_args) if function_args else {}
        except json.JSONDecodeError as e:
            error_code = "MALFORMED_FUNCTION_CALL"
            error_message = f"invalid arguments JSON for {function_name}: {e}"

    return LLMResponse(
        content=LLMResponseContent(
            parts=[
                LLMResponsePart(
                    function_call=FunctionCall(
                        id=call_id or "",
                        name=function_name or "",
                        args=args,
                    ),
                )
            ]
        ),
        error_code=error_code,
        error_message=error_message,
    )