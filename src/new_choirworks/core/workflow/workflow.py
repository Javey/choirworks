# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_workflow.py
from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import override

from pydantic import Field, PrivateAttr

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.base_node import START, BaseNode
from new_choirworks.core.workflow.errors import WorkflowError
from new_choirworks.core.workflow.graph import Edge, Graph
from new_choirworks.core.workflow.trigger import Trigger


class Workflow(BaseNode):
    """A graph-based workflow node.

    Design reference: google-adk Workflow (Apache-2.0, Copyright 2026 Google LLC).
    Simplified: no trigger buffer replay, no NodeRunner, no child Context,
    no retry/abort/interrupt/resume.

    _run_impl() is the graph orchestration loop:
    - SETUP: build graph, seed triggers
    - LOOP: schedule ready nodes, handle completions
    - FINALIZE: store node outputs
    """

    edges: list[Edge] = Field(default_factory=list)
    max_concurrency: int | None = None
    timeout: float | None = None

    _graph: Graph | None = PrivateAttr(default=None)

    @override
    def model_post_init(self, context: object) -> None:
        super().model_post_init(context)
        self._graph = Graph(edges=self.edges)
        self._graph.validate_graph()

    def _get_static_node_by_name(self, name: str) -> BaseNode:
        """Find a node in the graph by name."""
        assert self._graph is not None
        for node in self._graph.nodes:
            if node.name == name:
                return node
        raise WorkflowError(f"node {name} not found in graph")

    def _seed_start_triggers(
        self,
        trigger_buffer: dict[str, list[Trigger]],
    ) -> None:
        """Seed triggers for START's direct successors."""
        assert self._graph is not None
        next_nodes = self._graph.get_next_pending_nodes(START.name, None)
        for name in next_nodes:
            trigger_buffer.setdefault(name, []).append(Trigger())

    @override
    async def _run_impl(
        self, _ctx: InvocationContext, _node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        """Placeholder — implemented in subsequent steps."""
        return
        yield  # pyright: ignore[reportUnreachable]
