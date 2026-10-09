# 设计参考 google-adk agents/llm_agent.py（Apache-2.0, Copyright 2026 Google LLC）：
# 仅借鉴 SingleFlow 语义（单次结构化 LLM 调用 + 失败反馈重试）；不搬 AutoFlow
# 工具循环、model / generate_content_config（共用 ctx.llm）、回调体系。
from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import override

import structlog
from litellm.types.utils import Delta

from choirworks.core.agents.base import BaseAgent
from choirworks.core.agents.context import TurnContext
from choirworks.core.events import TEXT, THOUGHT, chunk_event, emit
from choirworks.core.llm import ToolParseError
from choirworks.tools.base import AgentFunction, ToolCallResult

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class LlmAgent[T](BaseAgent):
    """LLM 子 agent（ADK SingleFlow 语义）。

    ``run_async`` 吃一段拼好的提示词，思考/正文 chunk 边跑边推事件
    （``cw_type`` 词汇表，author 为 agent 名，每轮尝试独立 artifact、
    结束时 seal）；模型未调工具或解析失败时追加反馈重试；重试耗尽走
    ``process(None)`` 兜底，成功返回 ``process(tool_call)``。
    """

    system_prompt: str
    tool_name: str
    build_tools: Callable[..., Awaitable[list[AgentFunction]]]
    process: Callable[[ToolCallResult | None], T]
    max_retries: int = 2

    @override
    async def run_async(self, ctx: TurnContext, user: str, **tool_kwargs: object) -> T:
        logger.info(
            "llm_agent",
            name=self.name,
            user=user,
            retries=self.max_retries,
        )
        last_error: Exception | None = None
        current_user = user
        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                logger.warning(
                    "llm_agent validation failed",
                    name=self.name,
                    attempt=attempt,
                    max_attempts=self.max_retries + 1,
                    error=last_error,
                )
            tools = await self.build_tools(ctx, **tool_kwargs)
            thought_id = uuid.uuid4().hex
            text_id = uuid.uuid4().hex
            tool_call: ToolCallResult | None = None
            reasoning_parts: list[str] = []
            content_parts: list[str] = []
            try:
                # ctx 类型缝：接线期业务回合子类就位后移除 ignore。
                async for item in ctx.llm.stream(
                    system=self.system_prompt,
                    user=current_user,
                    tools=tools,
                    ctx=ctx,  # pyright: ignore[reportArgumentType]
                    tool_choice={
                        "type": "function",
                        "function": {"name": self.tool_name},
                    },
                ):
                    if isinstance(item, ToolCallResult):
                        tool_call = item
                        continue
                    if isinstance(item, Delta):
                        reasoning = item.reasoning_content or ""
                        content = item.content or ""
                        if reasoning:
                            reasoning_parts.append(reasoning)
                            await emit(
                                ctx,
                                chunk_event(
                                    ctx.task_id,
                                    ctx.context_id,
                                    text=reasoning,
                                    kind=THOUGHT,
                                    author=self.name,
                                    artifact_id=thought_id,
                                    append=len(reasoning_parts) > 1,
                                    last_chunk=False,
                                ),
                            )
                        if content:
                            content_parts.append(content)
                            await emit(
                                ctx,
                                chunk_event(
                                    ctx.task_id,
                                    ctx.context_id,
                                    text=content,
                                    kind=TEXT,
                                    author=self.name,
                                    artifact_id=text_id,
                                    append=len(content_parts) > 1,
                                    last_chunk=False,
                                ),
                            )
                if tool_call is None:
                    raise ValueError(f"model did not call the {self.tool_name} tool")
            except (ValueError, ToolParseError) as exc:
                last_error = exc
                await self._seal_attempt(ctx, thought_id, text_id, reasoning_parts, content_parts)
                if isinstance(exc, ToolParseError):
                    prev_payload = exc.payload
                elif tool_call is not None:
                    prev_payload = tool_call.args.model_dump_json()
                else:
                    prev_payload = ""
                current_user += (
                    f"\n\nYour previous tool call was invalid.\n"
                    f"Tool call args:\n{prev_payload}\n\n"
                    f"Error: {exc}\n"
                    "Return a corrected tool call."
                )
                continue
            await self._seal_attempt(ctx, thought_id, text_id, reasoning_parts, content_parts)
            logger.info("llm_agent done", name=self.name, tool=self.tool_name)
            return self.process(tool_call)
        logger.warning("llm_agent exhausted", name=self.name, retries=self.max_retries)
        return self.process(None)

    async def _seal_attempt(
        self,
        ctx: TurnContext,
        thought_id: str,
        text_id: str,
        reasoning_parts: list[str],
        content_parts: list[str],
    ) -> None:
        """Seal this attempt's chunk artifacts（对齐 stream_plan 的收尾行为）。"""
        reasoning = "".join(reasoning_parts)
        content = "".join(content_parts)
        if reasoning:
            await emit(
                ctx,
                chunk_event(
                    ctx.task_id,
                    ctx.context_id,
                    text=reasoning,
                    kind=THOUGHT,
                    author=self.name,
                    artifact_id=thought_id,
                    append=False,
                    last_chunk=True,
                ),
            )
        if content:
            await emit(
                ctx,
                chunk_event(
                    ctx.task_id,
                    ctx.context_id,
                    text=content,
                    kind=TEXT,
                    author=self.name,
                    artifact_id=text_id,
                    append=False,
                    last_chunk=True,
                ),
            )
