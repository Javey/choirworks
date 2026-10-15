# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/utils/_graph_validation.py
from __future__ import annotations

from collections import Counter

from new_choirworks.core.workflow.base_node import START, BaseNode
from new_choirworks.core.workflow.errors import GraphValidationError
from new_choirworks.core.workflow.graph import DEFAULT_ROUTE, Edge, RouteValue


def validate_graph(nodes: list[BaseNode], edges: list[Edge]) -> None:
    """Validate the graph structure. Raises GraphValidationError on failure."""
    node_names = _validate_duplicate_node_names(nodes)
    _validate_start_node(node_names)
    _validate_start_edges(edges)
    _validate_connectivity(edges, node_names)
    _validate_duplicate_edges(edges)
    _validate_default_routes(edges)
    _detect_unconditional_cycles(edges, node_names)


def _validate_duplicate_node_names(nodes: list[BaseNode]) -> set[str]:
    names = [node.name for node in nodes]
    duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
    if duplicates:
        raise GraphValidationError(f"duplicate node names: {duplicates}")
    return set(names)


def _validate_start_node(node_names: set[str]) -> None:
    if START.name not in node_names:
        raise GraphValidationError("START node not found in graph nodes")


def _validate_start_edges(edges: list[Edge]) -> None:
    for edge in edges:
        if edge.from_node.name == START.name and edge.route is not None:
            raise GraphValidationError(
                f"edges from START must not have routes (edge to {edge.to_node.name} has route {edge.route!r})"  # noqa: E501
            )


def _validate_connectivity(edges: list[Edge], node_names: set[str]) -> None:
    adj: dict[str, set[str]] = {name: set() for name in node_names}
    to_nodes: set[str] = set()
    for edge in edges:
        adj[edge.from_node.name].add(edge.to_node.name)
        to_nodes.add(edge.to_node.name)

    reachable: set[str] = set()
    stack = [START.name]
    while stack:
        node = stack.pop()
        if node in reachable:
            continue
        reachable.add(node)
        stack.extend(adj[node] - reachable)

    unreachable = node_names - reachable
    if unreachable:
        raise GraphValidationError(f"unreachable from START: {sorted(unreachable)}")
    if START.name in to_nodes:
        raise GraphValidationError("START node must not have incoming edges")


def _normalize_route(
    route: RouteValue | list[RouteValue] | None,
) -> set[RouteValue | None]:
    if route is None:
        return {None}
    if isinstance(route, list):
        return set(route)
    return {route}


def _validate_duplicate_edges(edges: list[Edge]) -> None:
    seen: dict[tuple[str, str], set[RouteValue | None]] = {}
    for edge in edges:
        key = (edge.from_node.name, edge.to_node.name)
        new_routes = _normalize_route(edge.route)
        if key in seen:
            existing = seen[key]
            if None in existing or None in new_routes or not existing.isdisjoint(new_routes):
                raise GraphValidationError(
                    f"duplicate edge: {edge.from_node.name} -> {edge.to_node.name}"
                )
            existing.update(new_routes)
        else:
            seen[key] = new_routes


def _validate_default_routes(edges: list[Edge]) -> None:
    for edge in edges:
        if isinstance(edge.route, list) and DEFAULT_ROUTE in edge.route:
            raise GraphValidationError(
                f"DEFAULT_ROUTE cannot be combined with other routes (edge {edge.from_node.name} -> {edge.to_node.name})"  # noqa: E501
            )


def _detect_unconditional_cycles(edges: list[Edge], node_names: set[str]) -> None:
    adj: dict[str, list[str]] = {name: [] for name in node_names}
    for edge in edges:
        if edge.route is None:
            adj[edge.from_node.name].append(edge.to_node.name)

    in_stack: set[str] = set()
    done: set[str] = set()

    def _dfs(node: str, path: list[str]) -> None:
        in_stack.add(node)
        path.append(node)
        for neighbor in adj[node]:
            if neighbor in in_stack:
                cycle_start = path.index(neighbor)
                cycle = path[cycle_start:] + [neighbor]
                raise GraphValidationError(f"unconditional cycle detected: {' -> '.join(cycle)}")
            if neighbor not in done:
                _dfs(neighbor, path)
        _ = path.pop()
        in_stack.remove(node)
        done.add(node)

    for name in node_names:
        if name not in done:
            _dfs(name, [])
