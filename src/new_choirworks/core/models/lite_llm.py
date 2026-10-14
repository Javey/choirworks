import copy
import json
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import cast, override

import litellm
from litellm.litellm_core_utils.streaming_handler import CustomStreamWrapper
from litellm.types.llms.openai import OpenAIChatCompletionSystemMessage
from litellm.types.utils import (
    ChatCompletionDeltaToolCall,
    ChatCompletionMessageToolCall,
    Delta,
    Message,
    ModelResponse,
)
from pydantic import BaseModel

from .base_llm import BaseLLM
from .llm_request import LLMRequest
from .llm_response import FunctionCall, LLMResponse, LLMResponseContent, LLMResponsePart

logger = logging.getLogger(__name__)

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
        messages = list(llm_request.messages)
        if llm_request.system_instruction:
            messages.insert(
                0,
                OpenAIChatCompletionSystemMessage(
                    role="system", content=llm_request.system_instruction
                ),
            )
        kwargs: dict[str, object] = {
            "model": effective_model,
            "messages": messages,
            "stream": stream,
            "base_url": self.base_url,
        }
        if llm_request.tools:
            kwargs["tools"] = llm_request.tools
        if llm_request.response_schema:
            kwargs["response_format"] = _to_response_format(llm_request.response_schema)

        logger.info(
            "LLM call: model=%s, messages=%d, tools=[%s], stream=%s",
            effective_model,
            len(messages),
            ", ".join(t["function"]["name"] for t in llm_request.tools)
            if llm_request.tools
            else "",
            stream,
        )

        response = await self.completion_fn(**kwargs)

        _gen = _stream(response) if stream else _text(response)
        async for chunk in _gen:
            yield chunk


async def _stream(response: CompletionResult) -> AsyncGenerator[LLMResponse, None]:
    """处理 CustomStreamWrapper 类型的响应，生成 LLMResponse。"""
    if not isinstance(response, CustomStreamWrapper):
        raise ValueError("Expected a CustomStreamWrapper for streaming response.")
    tool_call_buffer: dict[int, _ToolCallBuffer] = {}
    async for chunk in response:
        if not chunk.choices:
            continue
        # print(chunk)
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
                function_name=buffer.name, function_args="".join(buffer.args), call_id=buffer.id
            )


async def _text(response: CompletionResult) -> AsyncGenerator[LLMResponse, None]:
    """处理 ModelResponse 类型的响应，生成 LLMResponse。"""
    if not isinstance(response, ModelResponse):
        raise ValueError("Expected a ModelResponse for non-streaming response.")
    message = response.choices[0].message
    parts = _build_parts(message)
    if parts:
        yield LLMResponse(content=LLMResponseContent(parts=parts))
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


def _enforce_strict_openai_schema(schema: dict[str, object]) -> None:
    """Recursively transforms a JSON schema for OpenAI strict structured outputs.

    OpenAI strict mode requires:
    1. additionalProperties: false on all object schemas (including nested/$defs).
    2. All properties listed in 'required' (no optional omissions).
    3. $ref nodes must have no sibling keywords (e.g., no 'description' next to '$ref').

    This function mutates the schema dict in place.

    Design reference: google-adk models/lite_llm.py _enforce_strict_openai_schema
    (Apache-2.0, Copyright 2026 Google LLC).
    """
    if "$ref" in schema:
        for key in list(schema.keys()):
            if key != "$ref":
                del schema[key]
        return

    if schema.get("type") == "object" and "properties" in schema:
        schema["additionalProperties"] = False
        properties = cast(dict[str, object], schema["properties"])
        schema["required"] = sorted(properties.keys())

    defs = cast(dict[str, object], schema.get("$defs", {}))
    for defn in defs.values():
        if isinstance(defn, dict):
            _enforce_strict_openai_schema(cast(dict[str, object], defn))

    props = cast(dict[str, object], schema.get("properties", {}))
    for prop in props.values():
        if isinstance(prop, dict):
            _enforce_strict_openai_schema(cast(dict[str, object], prop))

    for key in ("anyOf", "oneOf", "allOf"):
        items = cast(list[object], schema.get(key, []))
        for item in items:
            if isinstance(item, dict):
                _enforce_strict_openai_schema(cast(dict[str, object], item))

    if "items" in schema and isinstance(schema["items"], dict):
        _enforce_strict_openai_schema(cast(dict[str, object], schema["items"]))


def _to_response_format(schema: type[BaseModel]) -> dict[str, object]:
    """Convert a Pydantic model to OpenAI json_schema response_format.

    Design reference: google-adk models/lite_llm.py _to_litellm_response_format
    (Apache-2.0, Copyright 2026 Google LLC).
    """
    schema_dict: dict[str, object] = copy.deepcopy(schema.model_json_schema())
    _enforce_strict_openai_schema(schema_dict)
    return {
        "type": "json_schema",
        "json_schema": {
            "name": schema.__name__,
            "strict": True,
            "schema": schema_dict,
        },
    }
