# 设计参考 google-adk agents/llm_agent.py（Apache-2.0, Copyright 2026 Google LLC）：
# 借鉴双态形状——终态工具决策（对应 output_schema / set_model_response）与
# 无工具文字回复；不搬 AutoFlow 多工具循环、model / generate_content_config
# （共用 ctx.llm）、回调体系。
from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from typing import cast, override

import structlog
from litellm.types.utils import Delta

from choirworks.core.agents.base import BaseAgent
from choirworks.core.agents.context import TurnContext
from choirworks.core.events import AgentEvent, ChunkKind, CwType, ResultEvent, chunk_event
from choirworks.core.llm import ToolParseError
from choirworks.core.tool import FunctionTool, ToolCallResult

logger = structlog.get_logger(__name__)


class LlmAgent[T](BaseAgent):
    """LLM 子 agent（双态，ADK LlmAgent 形状）。

    **决策态**：子类覆写 :meth:`build_tools` / :meth:`process` 并设类属性
    ``final_tool``——强制调用终态工具（工具参数即输出），思考/正文 chunk 边
    跑边 yield 事件（``cw_type`` 词汇表，author 为 agent 名，每轮尝试独立
    artifact、结束时 seal）；模型未调工具或解析失败时追加反馈重试，重试
    耗尽走 ``process(None)`` 兜底，成功返回 ``process(tool_call)``。

    **文字态**：``final_tool`` 为 ``None``（默认）——不声明工具的单次文字
    回复（ADK 无工具默认路径），思考/正文照流，返回拼接文本。

    子类可直接设类属性 ``system_prompt`` / ``final_tool`` / ``max_retries``
    作为默认值；构造时传入同名关键字参数可覆盖。
    """

    final_tool: str | None = None
    max_retries: int = 2

    def __init__(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        sub_agents: list[BaseAgent] | None = None,
        system_prompt: str | None = None,
        final_tool: str | None = None,
        max_retries: int | None = None,
    ) -> None:
        super().__init__(name=name, description=description, sub_agents=sub_agents)
        if system_prompt is not None:
            self.system_prompt = system_prompt
        if not hasattr(self, "system_prompt"):
            raise TypeError(f"{type(self).__name__} requires a 'system_prompt'")
        if final_tool is not None:
            self.final_tool = final_tool
        if max_retries is not None:
            self.max_retries = max_retries

    async def build_tools(self, ctx: TurnContext, **kwargs: object) -> list[FunctionTool]:
        """决策态子类覆写：构建可供模型调用的工具列表。"""
        return []

    def process(self, tool_call: ToolCallResult | None) -> T:
        """决策态子类覆写：将工具调用结果（或 ``None`` 兜底）转为类型化输出。"""
        return cast("T", None)

    @override
    async def run_async(
        self, ctx: TurnContext, user: str, **tool_kwargs: object
    ) -> AsyncGenerator[AgentEvent]:
        final_tool = self.final_tool
        if final_tool is None:
            async for event in self._run_text(ctx, user):
                yield event
            return
        logger.info(
            "llm_agent",
            name=self.name,
            user=user,
            tool=final_tool,
            retries=self.max_retries,
        )
        last_error: Exception | None = None
        current_user = user
        thought_id = ""
        text_id = ""
        reasoning_parts: list[str] = []
        content_parts: list[str] = []
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
            reasoning_parts = []
            content_parts = []
            try:
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
                        reasoning = getattr(item, "reasoning_content", None) or ""
                        content = item.content or ""
                        if reasoning:
                            reasoning_parts.append(reasoning)
                            yield self._chunk(
                                ctx,
                                text=reasoning,
                                kind=CwType.THOUGHT,
                                artifact_id=thought_id,
                                append=len(reasoning_parts) > 1,
                                last_chunk=False,
                            )
                        if content:
                            content_parts.append(content)
                            yield self._chunk(
                                ctx,
                                text=content,
                                kind=CwType.TEXT,
                                artifact_id=text_id,
                                append=len(content_parts) > 1,
                                last_chunk=False,
                            )
                if tool_call is None:
                    raise ValueError(f"model did not call the {final_tool} tool")
            except (ValueError, ToolParseError) as exc:
                last_error = exc
                for ev in self._seal(ctx, thought_id, text_id, reasoning_parts, content_parts):
                    yield ev
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
            for ev in self._seal(ctx, thought_id, text_id, reasoning_parts, content_parts):
                yield ev
            logger.info("llm_agent done", name=self.name, tool=final_tool)
            yield ResultEvent(self.process(tool_call))
            return
        logger.warning("llm_agent exhausted", name=self.name, retries=self.max_retries)
        for ev in self._seal(ctx, thought_id, text_id, reasoning_parts, content_parts):
            yield ev
        yield ResultEvent(self.process(None))

    async def _run_text(self, ctx: TurnContext, user: str) -> AsyncGenerator[AgentEvent]:
        """文字态：无工具单次回复，流式 yield 思考/正文，最后 yield 拼接文本。"""
        logger.info("llm_agent text", name=self.name, user=user)
        thought_id = uuid.uuid4().hex
        text_id = uuid.uuid4().hex
        reasoning_parts: list[str] = []
        content_parts: list[str] = []
        async for item in ctx.llm.stream(system=self.system_prompt, user=user):
            if isinstance(item, Delta):
                reasoning = getattr(item, "reasoning_content", None) or ""
                content = item.content or ""
                if reasoning:
                    reasoning_parts.append(reasoning)
                    yield self._chunk(
                        ctx,
                        text=reasoning,
                        kind=CwType.THOUGHT,
                        artifact_id=thought_id,
                        append=len(reasoning_parts) > 1,
                        last_chunk=False,
                    )
                if content:
                    content_parts.append(content)
                    yield self._chunk(
                        ctx,
                        text=content,
                        kind=CwType.TEXT,
                        artifact_id=text_id,
                        append=len(content_parts) > 1,
                        last_chunk=False,
                    )
        for ev in self._seal(ctx, thought_id, text_id, reasoning_parts, content_parts):
            yield ev
        yield ResultEvent(cast("T", "".join(content_parts)))

    def _chunk(
        self,
        ctx: TurnContext,
        *,
        text: str,
        kind: ChunkKind,
        artifact_id: str,
        append: bool,
        last_chunk: bool,
    ) -> AgentEvent:
        """Build a chunk_event with agent's identity baked in."""
        return chunk_event(
            ctx.task_id,
            ctx.context_id,
            text=text,
            kind=kind,
            author=self.name,
            artifact_id=artifact_id,
            append=append,
            last_chunk=last_chunk,
        )

    def _seal(
        self,
        ctx: TurnContext,
        thought_id: str,
        text_id: str,
        reasoning_parts: list[str],
        content_parts: list[str],
    ) -> list[AgentEvent]:
        """Seal this attempt's chunk artifacts（对齐 stream_plan 的收尾行为）。"""
        events: list[AgentEvent] = []
        reasoning = "".join(reasoning_parts)
        content = "".join(content_parts)
        if reasoning:
            events.append(
                self._chunk(
                    ctx,
                    text=reasoning,
                    kind=CwType.THOUGHT,
                    artifact_id=thought_id,
                    append=False,
                    last_chunk=True,
                )
            )
        if content:
            events.append(
                self._chunk(
                    ctx,
                    text=content,
                    kind=CwType.TEXT,
                    artifact_id=text_id,
                    append=False,
                    last_chunk=True,
                )
            )
        return events
