# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_graph.py
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

from new_choirworks.core.workflow.base_node import START, BaseNode, RouteValue
from new_choirworks.core.workflow.errors import GraphValidationError

logger = logging.getLogger(__name__)

DEFAULT_ROUTE = "__DEFAULT__"

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

    def get_next_pending_nodes(
        self,
        node_name: str,
        routes_to_match: RouteValue | list[RouteValue] | None,
    ) -> list[str]:
        """Determine next nodes to trigger based on routes emitted by node_name.

        - Edges with route=None are always triggered.
        - Edges with a specific route are triggered when routes_to_match matches.
        - DEFAULT_ROUTE edges fire only when no specific route matched.
        - If no edges match and routing edges exist, warns and returns [].
        """
        next_pending_nodes: list[str] = []
        matched_specific_route = False
        default_route_nodes: list[str] = []
        has_routing_edges = False

        for edge in self.edges:
            if edge.from_node.name != node_name:
                continue
            # Edges with no route tag are always triggered.
            if edge.route is None:
                next_pending_nodes.append(edge.to_node.name)
                continue

            has_routing_edges = True

            # DEFAULT_ROUTE edges are fallback — only fired when no specific match.
            if edge.route == DEFAULT_ROUTE:
                default_route_nodes.append(edge.to_node.name)
                continue

            # Normalize edge routes to a set for matching.
            edge_routes = set(edge.route) if isinstance(edge.route, list) else {edge.route}

            edge_matched = False
            if isinstance(routes_to_match, list):
                if edge_routes & set(routes_to_match):
                    edge_matched = True
            elif routes_to_match in edge_routes:
                edge_matched = True

            if edge_matched:
                next_pending_nodes.append(edge.to_node.name)
                matched_specific_route = True

        # DEFAULT_ROUTE edges fire only when no specific route matched.
        if not matched_specific_route:
            next_pending_nodes.extend(default_route_nodes)

        if has_routing_edges and not next_pending_nodes:
            logger.warning(
                "Node '%s' has conditional edges but none matched route(s): %s",
                node_name,
                routes_to_match,
            )

        return next_pending_nodes
