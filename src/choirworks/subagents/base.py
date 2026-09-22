from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from choirworks.core.llm import LiteLLMClient
from choirworks.orchestration.context import OrchestrationContext
from choirworks.tools.base import AgentFunction, FunctionContext, ToolCallResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Subagent[T]:
    """LLM-backed orchestration subagent with its own prompt + tools.

    A data-only record bundling a system prompt, a set of tools, a result
    post-processor and a retry budget.  The shared stream + retry loop lives
    in :func:`run_subagent`, similar to ADK's LlmAgent but without the
    inheritance.

    ``process`` maps the tool call (or ``None`` when the model never called
    the tool) to the caller-facing result, so "default on failure" policies
    stay declarative.
    """

    name: str
    system_prompt: str
    tool_name: str
    build_tools: Callable[..., Awaitable[list[AgentFunction]]]
    process: Callable[[ToolCallResult | None], T]
    max_retries: int = 2


async def run_subagent[T](
    llm: LiteLLMClient,
    subagent: Subagent[T],
    ctx: OrchestrationContext,
    user: str,
    **tool_kwargs: object,
) -> T:
    """Stream the model's reply, retrying on invalid tool calls."""
    logger.info(
        "run_subagent: name=%s user_len=%d retries=%d",
        subagent.name, len(user), subagent.max_retries,
    )
    func_ctx = FunctionContext(
        runtime=ctx.runtime,
        registry=ctx.registry,
        effects=ctx.effects,
    )
    last_error: Exception | None = None
    current_user = user
    for attempt in range(subagent.max_retries + 1):
        if attempt > 0:
            logger.warning(
                "%s validation failed (attempt %d/%d): %s",
                subagent.name, attempt, subagent.max_retries + 1, last_error,
            )
        tools = await subagent.build_tools(ctx, **tool_kwargs)
        tool_call: ToolCallResult | None = None
        try:
            async for item in llm.stream(
                system=subagent.system_prompt,
                user=current_user,
                tools=tools,
                ctx=func_ctx,
                tool_choice={"type": "function", "function": {"name": subagent.tool_name}},
            ):
                if isinstance(item, ToolCallResult):
                    tool_call = item
            if tool_call is None:
                raise ValueError(f"model did not call the {subagent.tool_name} tool")
        except ValueError as exc:
            last_error = exc
            current_user += (
                f"\n\nPrevious call was invalid: {exc}."
                " Return a corrected result."
            )
            continue
        logger.info(
            "run_subagent done: name=%s tool=%s",
            subagent.name, subagent.tool_name,
        )
        return subagent.process(tool_call)
    logger.warning(
        "run_subagent exhausted: name=%s retries=%d",
        subagent.name, subagent.max_retries,
    )
    return subagent.process(None)
