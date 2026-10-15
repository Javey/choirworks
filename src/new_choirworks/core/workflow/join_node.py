# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_join_node.py
from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import override

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.base_node import BaseNode


class JoinNode(BaseNode):
    """A node that waits for all predecessors to complete before running.

    Design reference: google-adk JoinNode (Apache-2.0, Copyright 2026 Google LLC).
    The orchestrator buffers each predecessor's contribution and only triggers
    this node when all predecessors have completed. The aggregated inputs
    (dict of predecessor_name → output) are passed as node_input and
    transparently set as this node's output.
    """

    @property
    @override
    def _requires_all_predecessors(self) -> bool:
        return True

    @override
    async def _run_impl(
        self, _ctx: InvocationContext, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        """JoinNode passes through the aggregated inputs as its output."""
        self.output: object | None = node_input
        return
        yield  # pyright: ignore[reportUnreachable]
