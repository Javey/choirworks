# 设计参考 google-adk models/llm_response.py（Apache-2.0, Copyright 2026 Google LLC）：
# 借鉴 LlmResponse → LLMResponseContent → Part 三层结构，归一化 litellm Delta 的动态字段。
from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel


class LLMResponse(BaseModel):
    """LLM 响应：归一化后的结构化内容。

    设计参考 ADK LlmResponse，用 LLMResponseContent → parts[] 三层替代直接暴露 litellm Delta。
    LiteLLM.generate_content_async 负责 Delta → LLMResponsePart 转换。
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=True,
        alias_generator=to_camel,
        populate_by_name=True,
    )

    content: LLMResponseContent | None = None
    error_code: str | None = None
    error_message: str | None = None

    def get_function_calls(self) -> list[FunctionCall]:
        """返回所有 function_call parts。"""
        if not self.content:
            return []
        return [part.function_call for part in self.content.parts if part.function_call]

    @classmethod
    def from_function_response(
        cls, fc: FunctionCall, response: dict[str, object]
    ) -> LLMResponse:
        """Build an LLMResponse containing a single function_response part."""
        return cls(
            content=LLMResponseContent(
                parts=[
                    LLMResponsePart(
                        function_response=FunctionResponse(
                            id=fc.id,
                            name=fc.name or "",
                            response=response,
                        )
                    )
                ]
            )
        )


@dataclass
class FunctionCall:
    """模型发起的工具调用。"""

    id: str
    name: str
    args: dict[str, object] | None = field(default_factory=dict)


@dataclass
class FunctionResponse:
    """工具执行结果。"""

    id: str
    name: str
    response: dict[str, object]


@dataclass
class LLMResponsePart:
    """归一化的响应片段：正文、思考或工具调用。"""

    text: str | None = None
    thought: bool = False
    function_call: FunctionCall | None = None
    function_response: FunctionResponse | None = None


@dataclass
class LLMResponseContent:
    """模型响应内容，含 role 和 parts。"""

    parts: list[LLMResponsePart] = field(default_factory=list)
