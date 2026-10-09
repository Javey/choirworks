# 设计参考 google-adk agents/llm_agent.py（Apache-2.0, Copyright 2026 Google LLC）：
# 借鉴双态形状——终态工具决策（对应 output_schema / set_model_response）与
# 无工具文字回复；不搬 AutoFlow 多工具循环、model / generate_content_config
# （共用 ctx.llm）、回调体系。
from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import cast, override

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
    """LLM 子 agent（双态，ADK LlmAgent 形状）。

    **决策态**：``build_tools`` / ``final_tool`` / ``process`` 三字段同现——
    强制调用终态工具（工具参数即输出），思考/正文 chunk 边跑边推事件
    （``cw_type`` 词汇表，author 为 agent 名，每轮尝试独立 artifact、结束时
    seal）；模型未调工具或解析失败时追加反馈重试，重试耗尽走 ``process(None)``
    兜底，成功返回 ``process(tool_call)``。

    **文字态**：三字段同缺——不声明工具的单次文字回复（ADK 无工具默认
    路径），思考/正文照流，返回拼接文本（``LlmAgent[str]``）。
    """

    system_prompt: str
    build_tools: Callable[..., Awaitable[list[AgentFunction]]] | None = None
    final_tool: str | None = None
    process: Callable[[ToolCallResult | None], T] | None = None
    max_retries: int = 2

    def __post_init__(self) -> None:
        # slots=True 的 dataclass 会重建类，零参 super() 的 __class__ 单元失效。
        super(LlmAgent, self).__post_init__()
        declared = (
            self.build_tools is not None,
            self.final_tool is not None,
            self.process is not None,
        )
        if any(declared) and not all(declared):
            raise ValueError(
                "decision mode requires build_tools / final_tool / process together; "
                "leave all three unset for text mode"
            )

    @override
    async def run_async(self, ctx: TurnContext, user: str, **tool_kwargs: object) -> T:
        build_tools = self.build_tools
        final_tool = self.final_tool
        process = self.process
        if final_tool is None:
            return await self._run_text(ctx, user)
        if build_tools is None or process is None:
            raise ValueError("decision mode requires build_tools / final_tool / process")
        logger.info(
            "llm_agent",
            name=self.name,
            user=user,
            tool=final_tool,
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
            tools = await build_tools(ctx, **tool_kwargs)
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
                        "function": {"name": final_tool},
                    },
                ):
                    if isinstance(item, ToolCallResult):
                        tool_call = item
                        continue
                    if isinstance(item, Delta):
                        await self._stream_delta(
                            ctx,
                            item,
                            thought_id,
                            text_id,
                            reasoning_parts,
                            content_parts,
                        )
                if tool_call is None:
                    raise ValueError(f"model did not call the {final_tool} tool")
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
            logger.info("llm_agent done", name=self.name, tool=final_tool)
            return process(tool_call)
        logger.warning("llm_agent exhausted", name=self.name, retries=self.max_retries)
        return process(None)

    async def _run_text(self, ctx: TurnContext, user: str) -> T:
        """文字态：无工具单次回复，流式推思考/正文，返回拼接文本。"""
        logger.info("llm_agent text", name=self.name, user=user)
        thought_id = uuid.uuid4().hex
        text_id = uuid.uuid4().hex
        reasoning_parts: list[str] = []
        content_parts: list[str] = []
        async for item in ctx.llm.stream(system=self.system_prompt, user=user):
            if isinstance(item, Delta):
                await self._stream_delta(
                    ctx, item, thought_id, text_id, reasoning_parts, content_parts
                )
        await self._seal_attempt(ctx, thought_id, text_id, reasoning_parts, content_parts)
        return cast("T", "".join(content_parts))

    async def _stream_delta(
        self,
        ctx: TurnContext,
        delta: Delta,
        thought_id: str,
        text_id: str,
        reasoning_parts: list[str],
        content_parts: list[str],
    ) -> None:
        reasoning = getattr(delta, "reasoning_content", None) or ""
        content = delta.content or ""
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
