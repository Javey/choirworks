# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_base_node.py
from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import aclosing
from typing import ClassVar

from pydantic import BaseModel, ConfigDict

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent


class BaseNode(BaseModel):
    """A base class for all nodes in the workflow graph.

    Design reference: google-adk BaseNode (Apache-2.0, Copyright 2026 Google LLC).
    Simplified: no schema validation, no interrupt/resume,
    no retry_config/timeout (managed by Workflow), no node_input parameter
    (predecessor outputs passed via context).
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    name: str
    description: str = ""

    async def run(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        """Execute the node, yielding A2A events.

        Wraps _run_impl with Aclosing to ensure cleanup on cancellation.
        """
        async with aclosing(self._run_impl(ctx)) as agen:
            async for event in agen:
                yield event

    async def _run_impl(self, _ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        """Override point for node execution logic.

        Raises NotImplementedError by default. BaseNode must stay
        constructible (START relies on this).
        """
        raise NotImplementedError(f"_run_impl for {type(self).__name__} is not implemented.")
        yield  # pyright: ignore[reportUnreachable]


START = BaseNode(name="__START__")
"""Sentinel node marking the entry point of a workflow graph.

START is never executed. Its successors are seeded directly by Workflow.
"""
