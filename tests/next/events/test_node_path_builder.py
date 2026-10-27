from __future__ import annotations

import pytest

from new_choirworks.core.events.node_path_builder import NodePathBuilder


def test_empty_path() -> None:
    path = NodePathBuilder.from_string("")
    assert str(path) == ""
    assert path.node_name == ""
    assert path.run_id is None
    assert path.leaf_segment == ""
    assert path.parent is None


def test_parse_and_properties() -> None:
    path = NodePathBuilder.from_string("wf@1/node_a@2")
    assert str(path) == "wf@1/node_a@2"
    assert path.node_name == "node_a"
    assert path.run_id == "2"
    assert path.leaf_segment == "node_a@2"
    assert path.static_path == "wf/node_a"


def test_parent_keeps_run_id() -> None:
    path = NodePathBuilder.from_string("wf@1/node_a@2")
    parent = path.parent
    assert parent is not None
    assert str(parent) == "wf@1"
    assert parent.run_id == "1"


def test_append_with_and_without_run_id() -> None:
    root = NodePathBuilder.from_string("wf@1")
    assert str(root.append("child", "3")) == "wf@1/child@3"
    assert str(root.append("child")) == "wf@1/child"


def test_descendant() -> None:
    child = NodePathBuilder.from_string("wf@1/node_a@2")
    assert child.is_descendant_of(NodePathBuilder.from_string("wf@1"))
    assert not child.is_descendant_of(NodePathBuilder.from_string("wf@1/node_a@2"))
    assert not child.is_descendant_of(NodePathBuilder.from_string("wf@2"))


def test_direct_child() -> None:
    child = NodePathBuilder.from_string("wf@1/node_a@2")
    parent = NodePathBuilder.from_string("wf@1")
    assert child.is_direct_child_of(parent)
    assert not parent.is_direct_child_of(child)
    assert not child.is_direct_child_of(child)


def test_includes_node_path() -> None:
    path = NodePathBuilder.from_string("wf@1")
    assert path.includes_node_path(None)
    assert path.includes_node_path("")
    assert path.includes_node_path("wf@1")
    assert path.includes_node_path("wf@1/node_a@2")
    assert not path.includes_node_path("wf@2/node_a@1")


def test_get_direct_child() -> None:
    path = NodePathBuilder.from_string("wf@1")
    descendant = NodePathBuilder.from_string("wf@1/node_a@2/leaf@1")
    assert str(path.get_direct_child(descendant)) == "wf@1/node_a@2"


def test_get_direct_child_rejects_non_descendant() -> None:
    path = NodePathBuilder.from_string("wf@1")
    with pytest.raises(ValueError):
        _ = path.get_direct_child(NodePathBuilder.from_string("other@2"))


def test_equality() -> None:
    assert NodePathBuilder.from_string("a@1") == NodePathBuilder.from_string("a@1")
    assert NodePathBuilder.from_string("a@1") != NodePathBuilder.from_string("a@2")
    assert NodePathBuilder.from_string("a@1") != "a@1"
