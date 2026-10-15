# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_graph.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from new_choirworks.core.workflow.base_node import START, BaseNode
from new_choirworks.core.workflow.errors import GraphValidationError

DEFAULT_ROUTE = "__DEFAULT__"

type RouteValue = bool | int | str

type NodeLike = BaseNode | Literal["START"]


@dataclass(frozen=True, slots=True)
class Edge:
    """An edge in the workflow graph.

    An edge with ``route=None`` is unconditional — always triggered when the
    source node completes. An edge with a specific route is only triggered
    when the source node emits that route. ``DEFAULT_ROUTE`` is the fallback
    taken when no specific route matched.
    """

    from_node: BaseNode
    to_node: BaseNode
    route: RouteValue | list[RouteValue] | None = None


@dataclass(slots=True)
class Graph:
    """A workflow graph.

    Nodes are inferred from edges; do not set ``nodes`` explicitly.
    Call ``validate_graph()`` after construction to check structural integrity.
    """

    edges: list[Edge] = field(default_factory=list)
    nodes: list[BaseNode] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.nodes:
            raise GraphValidationError(
                "Nodes are inferred from edges, do not set nodes explicitly."
            )
        seen: dict[int, BaseNode] = {}
        for edge in self.edges:
            for node in (edge.from_node, edge.to_node):
                _ = seen.setdefault(id(node), node)
        self.nodes = list(seen.values())

    def validate_graph(self) -> None:
        """Validate the graph structure. Raises GraphValidationError on failure."""
        from new_choirworks.core.workflow.utils.graph_validation import validate_graph

        validate_graph(self.nodes, self.edges)

    @property
    def terminal_node_names(self) -> set[str]:
        """Nodes with no outgoing edges (excluding START)."""
        from_names = {edge.from_node.name for edge in self.edges}
        return {
            node.name
            for node in self.nodes
            if node.name != START.name and node.name not in from_names
        }
