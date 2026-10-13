# 设计参考 google-adk models/llm_response.py（Apache-2.0, Copyright 2026 Google LLC）：
# 借鉴 LlmResponse → LLMResponseContent → Part 三层结构，归一化 litellm Delta 的动态字段。
from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class LLMResponsePart(BaseModel):
    """归一化的响应片段：正文或思考。"""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    text: str
    thought: bool = False


class LLMResponseContent(BaseModel):
    """模型响应内容，含 role 和 parts。"""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid")

    # role: str | None = None
    parts: list[LLMResponsePart] = Field(default_factory=list)


class LLMResponse(BaseModel):
    """LLM 响应：归一化后的结构化内容。

    设计参考 ADK LlmResponse，用 LLMResponseContent → parts[] 三层替代直接暴露 litellm Delta。
    LiteLLM.generate_content_async 负责 Delta → LLMResponsePart 转换。
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid",
        alias_generator=to_camel,
        populate_by_name=True,
    )

    content: LLMResponseContent | None = None
