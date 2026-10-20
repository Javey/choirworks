# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_base_node.py
from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import aclosing
from typing import TYPE_CHECKING, ClassVar

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from new_choirworks.core.events.event import A2AEvent
    from new_choirworks.core.workflow.context import Context

type RouteValue = bool | int | str


class BaseNode(BaseModel):
    """A base class for all nodes in the workflow graph.

    Design reference: google-adk BaseNode (Apache-2.0, Copyright 2026 Google LLC).
    Simplified: no schema validation, no interrupt/resume,
    no retry_config. timeout is on BaseNode (like ADK), read by NodeRunner.
    output/route are on Context (per-node), not on BaseNode fields.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    name: str
    description: str = ""
    timeout: float | None = None

    async def run(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        """Execute the node, yielding A2A events.

        Wraps _run_impl with Aclosing to ensure cleanup on cancellation.
        node_input is the output from the predecessor (or aggregated dict for JoinNode).
        """
        async with aclosing(self._run_impl(ctx, node_input)) as agen:
            async for event in agen:
                yield event

    async def _run_impl(
        self, _ctx: Context, _node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        """Override point for node execution logic.

        Raises NotImplementedError by default. BaseNode must stay
        constructible (START relies on this).
        """
        raise NotImplementedError(f"_run_impl for {type(self).__name__} is not implemented.")
        yield  # pyright: ignore[reportUnreachable]

    @property
    def _requires_all_predecessors(self) -> bool:
        return False


START = BaseNode(name="__START__")
"""Sentinel node marking the entry point of a workflow graph.

START is never executed. Its successors are seeded directly by Workflow.
"""
