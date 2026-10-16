# 设计参考 google-adk agents/base_agent.py（Apache-2.0, Copyright 2026 Google LLC）：
# BaseAgent(BaseNode) 层级——agent 即节点；run_async 是 agent 入口，_run_impl 是节点入口。
from __future__ import annotations

import abc
from collections.abc import AsyncGenerator
from contextlib import aclosing
from typing import TYPE_CHECKING, override

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.base_node import BaseNode

if TYPE_CHECKING:
    from new_choirworks.core.workflow.context import Context


class BaseAgent(BaseNode, abc.ABC):
    """An agent that can run standalone or as a workflow node.

    Design reference: google-adk BaseAgent(BaseNode) (Apache-2.0, Copyright 2026
    Google LLC). run_async() is the agent entry; the inherited run()/_run_impl()
    is the workflow-node entry.
    """

    async def run_async(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        """Run the agent asynchronously, yielding events as they occur."""
        async with aclosing(self._run_async_impl(ctx)) as agen:
            async for event in agen:
                yield event

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        """Runs the agent as a node in a workflow graph."""
        async for event in self.run_async(ctx.invocation_context):
            yield event

    @abc.abstractmethod
    def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        """Run the agent asynchronously, yielding events as they occur."""
        raise NotImplementedError("subclasses must implement _run_async_impl()")
