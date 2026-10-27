# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_node_runner.py
from __future__ import annotations

import asyncio
from contextlib import aclosing
from dataclasses import dataclass
from typing import TYPE_CHECKING

from new_choirworks.core.events.event import (
    A2AEvent,
    NodeInfo,
    is_partial_event,
    meta,
)
from new_choirworks.core.events.node_path_builder import NodePathBuilder
from new_choirworks.core.workflow.base_node import BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.errors import NodeTimeoutError

if TYPE_CHECKING:
    from new_choirworks.core.workflow.utils.replay_manager import ReplayManager


@dataclass
class NodeRunner:
    """Per-node executor. Drives BaseNode.run(), forwards events to event_queue.

    ADK returns a child Context with output/route/error.
    We return Context with output/route/error — same pattern, fewer fields.
    """

    node: BaseNode
    parent_ctx: Context
    run_id: str = "1"
    parent_path: str = ""
    replay_manager: ReplayManager | None = None

    def __post_init__(self) -> None:
        if not self.parent_path:
            self.parent_path = self.parent_ctx.node_path

    @property
    def node_path(self) -> str:
        base = NodePathBuilder.from_string(self.parent_path)
        return str(base.append(self.node.name, self.run_id))

    async def run(self, node_input: object | None = None) -> Context:
        """Execute node.run(), forwarding events to event_queue.

        Creates a child Context, executes the node, catches errors.
        Returns the Context with output/route/error set.
        """
        ctx = self._create_ctx()
        try:
            await self._execute_node(ctx, node_input)
        except Exception as e:
            ctx.error = e
        return ctx

    async def run_replayed(self, events: list[A2AEvent], output: object | None = None) -> Context:
        """Fast-forward a recorded node run without re-executing it.

        Recorded events are re-emitted to the stream but not appended again to
        the invocation event log. ``ReplayManager`` ordering is applied around
        the whole run.
        """
        ctx = self._create_ctx()
        key = f"{self.node.name}@{self.run_id}"
        if self.replay_manager is not None:
            await self.replay_manager.wait_sequence(self.parent_path, key)
        try:
            for event in events:
                await self._emit_replayed_event(event, ctx)
                event_output = meta(event).output
                if event_output is not None:
                    ctx.output = event_output
            if output is not None:
                ctx.output = output
        finally:
            if self.replay_manager is not None:
                await self.replay_manager.advance_sequence(self.parent_path, key)
        return ctx

    def _create_ctx(self) -> Context:
        return Context(
            invocation_context=self.parent_ctx.invocation_context,
            node=self.node,
            node_path=self.node_path,
            run_id=self.run_id,
            branch=self.parent_ctx.branch,
            isolation_scope=self.parent_ctx.isolation_scope,
        )

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

    async def _emit_replayed_event(self, event: A2AEvent, ctx: Context) -> None:
        self._enrich_event(event, ctx)
        if ctx.invocation_context.event_queue is not None:
            await ctx.invocation_context.event_queue.enqueue_event(event)

    def _enrich_event(self, event: A2AEvent, ctx: Context) -> None:
        """Set author / node_info / branch / isolation_scope on the event."""
        if not meta(event).author:
            meta(event).author = self.node.name

        node_info = meta(event).node_info
        if not node_info.path:
            meta(event).node_info = NodeInfo(
                path=ctx.node_path,
                output_for=node_info.output_for,
                message_as_output=node_info.message_as_output,
            )

        if meta(event).branch is None and ctx.branch is not None:
            meta(event).branch = ctx.branch
        if meta(event).isolation_scope is None and ctx.isolation_scope is not None:
            meta(event).isolation_scope = ctx.isolation_scope
