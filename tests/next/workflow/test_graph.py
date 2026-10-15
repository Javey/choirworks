import pytest

from new_choirworks.core.workflow.base_node import START, BaseNode
from new_choirworks.core.workflow.errors import GraphValidationError
from new_choirworks.core.workflow.graph import (
    DEFAULT_ROUTE,
    Edge,
    Graph,
)


def _node(name: str) -> BaseNode:
    return BaseNode(name=name)


# --- valid graphs ---


def test_valid_linear():
    a, b, c = _node("a"), _node("b"), _node("c")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(a, b),
            Edge(b, c),
        ]
    )
    graph.validate_graph()


def test_valid_with_route():
    a, b = _node("a"), _node("b")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(a, b, route="x"),
        ]
    )
    graph.validate_graph()


def test_valid_with_default_route():
    a, b, c = _node("a"), _node("b"), _node("c")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(a, b, route="x"),
            Edge(a, c, route=DEFAULT_ROUTE),
        ]
    )
    graph.validate_graph()


def test_valid_routed_cycle():
    a, b = _node("a"), _node("b")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(a, b, route="x"),
            Edge(b, a, route="retry"),
        ]
    )
    graph.validate_graph()


# --- validation errors ---


def test_duplicate_node_names():
    a1, a2, b = _node("a"), _node("a"), _node("b")
    graph = Graph(
        edges=[
            Edge(START, a1),
            Edge(a1, b),
            Edge(START, a2),
        ]
    )
    with pytest.raises(GraphValidationError, match="duplicate node names"):
        graph.validate_graph()


def test_missing_start():
    a, b = _node("a"), _node("b")
    graph = Graph(edges=[Edge(a, b)])
    with pytest.raises(GraphValidationError, match="START"):
        graph.validate_graph()


def test_start_edge_with_route():
    a = _node("a")
    graph = Graph(edges=[Edge(START, a, route="x")])
    with pytest.raises(GraphValidationError, match="must not have routes"):
        graph.validate_graph()


def test_start_has_incoming():
    a = _node("a")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(a, START),
        ]
    )
    with pytest.raises(GraphValidationError, match="incoming"):
        graph.validate_graph()


def test_unreachable():
    a, b, c = _node("a"), _node("b"), _node("c")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(b, c),
        ]
    )
    with pytest.raises(GraphValidationError, match="unreachable"):
        graph.validate_graph()


def test_duplicate_edge():
    a = _node("a")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(START, a),
        ]
    )
    with pytest.raises(GraphValidationError, match="duplicate edge"):
        graph.validate_graph()


def test_duplicate_edge_same_route():
    a, b = _node("a"), _node("b")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(a, b, route="x"),
            Edge(a, b, route="x"),
        ]
    )
    with pytest.raises(GraphValidationError, match="duplicate edge"):
        graph.validate_graph()


def test_duplicate_edge_different_route():
    a, b = _node("a"), _node("b")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(a, b),
            Edge(a, b, route="x"),
        ]
    )
    with pytest.raises(GraphValidationError, match="duplicate edge"):
        graph.validate_graph()


def test_default_in_list():
    a, b = _node("a"), _node("b")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(a, b, route=["x", DEFAULT_ROUTE]),
        ]
    )
    with pytest.raises(GraphValidationError, match="DEFAULT_ROUTE cannot be combined"):
        graph.validate_graph()


def test_unconditional_cycle():
    a, b = _node("a"), _node("b")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(a, b),
            Edge(b, a),
        ]
    )
    with pytest.raises(GraphValidationError, match="unconditional cycle"):
        graph.validate_graph()


def test_empty_graph():
    graph = Graph()
    with pytest.raises(GraphValidationError, match="START"):
        graph.validate_graph()


# --- construction ---


def test_nodes_inferred():
    a, b = _node("a"), _node("b")
    graph = Graph(edges=[Edge(START, a), Edge(a, b)])
    names = {n.name for n in graph.nodes}
    assert names == {"__START__", "a", "b"}


def test_explicit_nodes_rejected():
    a = _node("a")
    with pytest.raises(GraphValidationError, match="inferred from edges"):
        _ = Graph(nodes=[a])


def test_terminal_node_names():
    a, b, c = _node("a"), _node("b"), _node("c")
    graph = Graph(
        edges=[
            Edge(START, a),
            Edge(a, b),
            Edge(a, c),
        ]
    )
    assert graph.terminal_node_names == {"b", "c"}
