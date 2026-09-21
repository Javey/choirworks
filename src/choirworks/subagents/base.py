from __future__ import annotations

import logging

from pydantic import BaseModel, ValidationError

from choirworks.a2a.context import OrchestrationContext
from choirworks.core.llm import LiteLLMClient
from choirworks.core.planner import PlanValidationError
from choirworks.tools.base import AgentFunction, FunctionContext, ToolCallResult

logger = logging.getLogger(__name__)


class InternalSubagent:
    """LLM-backed orchestration subagent with its own prompt + tools.

    Similar to ADK's LlmAgent: a configuration object that bundles a
    system prompt, a set of tools, and a retry loop.  The ``run`` method
    calls :meth:`LiteLLMClient.stream`, streams ``Delta`` chunks to an
    optional ``stream_handler``, and returns a :class:`ToolCallResult`.

    Subclasses override :meth:`_build_tools` (dynamically construct tools
    per call) and optionally :meth:`_validate` (validate the tool-call args
    before accepting the result).
    """

    name: str
    system_prompt: str
    tool_name: str
    max_retries: int = 2

    def __init__(
        self,
        llm: LiteLLMClient,
        *,
        max_retries: int = 2,
    ):
        self._llm = llm
        self.max_retries = max_retries

    async def _build_tools(
        self, ctx: OrchestrationContext, **kwargs: object
    ) -> list[AgentFunction]:
        raise NotImplementedError

    def _validate(
        self, args: BaseModel, ctx: OrchestrationContext
    ) -> None:
        pass

    def _build_func_ctx(self, ctx: OrchestrationContext) -> FunctionContext:
        return FunctionContext(
            executor=ctx.executor, runtime=ctx.runtime,
        )  # type: ignore[arg-type]

    async def run(
        self,
        ctx: OrchestrationContext,
        user: str,
        *,
        stream_handler: object | None = None,
        **tool_kwargs: object,
    ) -> ToolCallResult | None:
        func_ctx = self._build_func_ctx(ctx)
        last_error: Exception | None = None
        current_user = user
        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                logger.warning(
                    "%s validation failed (attempt %d/%d): %s",
                    self.name, attempt, self.max_retries + 1, last_error,
                )
            tools = await self._build_tools(ctx, **tool_kwargs)
            tool_call: ToolCallResult | None = None
            try:
                async for item in self._llm.stream(
                    system=self.system_prompt,
                    user=current_user,
                    tools=tools,
                    ctx=func_ctx,
                    tool_choice={"type": "function", "function": {"name": self.tool_name}},
                ):
                    if isinstance(item, ToolCallResult):
                        tool_call = item
                    elif stream_handler is not None:
                        await stream_handler(item)
                if tool_call is None:
                    raise ValueError(f"model did not call the {self.tool_name} tool")
                self._validate(tool_call.args, ctx)
            except (ValidationError, ValueError, PlanValidationError) as exc:
                last_error = exc
                current_user += (
                    "\n\nPrevious call was invalid: {exc}."
                    " Return a corrected result."
                )
                continue
            return tool_call
        return None
