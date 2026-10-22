# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_node_runner.py
from __future__ import annotations

import asyncio
from contextlib import aclosing
from dataclasses import dataclass

from new_choirworks.core.events.event import A2AEvent, is_partial_event, meta
from new_choirworks.core.workflow.base_node import BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.errors import NodeTimeoutError


@dataclass
class NodeRunner:
    """Per-node executor. Drives BaseNode.run(), forwards events to event_queue.

    ADK returns a child Context with output/route/error.
    We return Context with output/route/error — same pattern, fewer fields.
    """

    node: BaseNode
    parent_ctx: Context

    async def run(self, node_input: object | None = None) -> Context:
        """Execute node.run(), forwarding events to event_queue.

        Creates a child Context, executes the node, catches errors.
        Returns the Context with output/route/error set.
        """
        ctx = Context(
            invocation_context=self.parent_ctx.invocation_context,
            node=self.node,
        )
        try:
            await self._execute_node(ctx, node_input)
        except Exception as e:
            ctx.error = e
        return ctx

    async def _execute_node(self, ctx: Context, node_input: object | None) -> None:
        timeout = self.node.timeout
        if timeout is not None:
            await self._run_node_loop_with_timeout(ctx, node_input, timeout)
        else:
            await self._run_node_loop(ctx, node_input)

    async def _run_node_loop(self, ctx: Context, node_input: object | None) -> None:
        async with aclosing(self.node.run(ctx, node_input=node_input)) as agen:
            async for event in agen:
                await self._enqueue_event(event, ctx)

    async def _run_node_loop_with_timeout(
        self,
        ctx: Context,
        node_input: object | None,
        timeout: float,
    ) -> None:
        try:
            async with asyncio.timeout(timeout):
                await self._run_node_loop(ctx, node_input)
        except TimeoutError as e:
            raise NodeTimeoutError(node_name=self.node.name, timeout=timeout) from e

    async def _enqueue_event(self, event: A2AEvent, ctx: Context) -> None:
        assert ctx.invocation_context.event_queue is not None
        self._enrich_event(event, ctx)
        if not is_partial_event(event):
            ctx.invocation_context.events.append(event)
        await ctx.invocation_context.event_queue.enqueue_event(event)

    def _enrich_event(self, event: A2AEvent, _ctx: Context) -> None:
        """Set author on the event (ADK also sets node_info/invocation_id)."""
        if not meta(event).author:
            meta(event).author = self.node.name
