from new_choirworks.core.workflow.base_node import START, BaseNode
from new_choirworks.core.workflow.graph import Edge
from new_choirworks.core.workflow.trigger import Trigger
from new_choirworks.core.workflow.workflow import Workflow


def _node(name: str) -> BaseNode:
    return BaseNode(name=name)


def test_seed_start_triggers_single():
    a = _node("a")
    workflow = Workflow(name="wf", edges=[Edge(START, a)])
    buffer: dict[str, list[Trigger]] = {}
    workflow._seed_start_triggers(buffer)  # pyright: ignore[reportPrivateUsage]
    assert "a" in buffer
    assert len(buffer["a"]) == 1
    assert isinstance(buffer["a"][0], Trigger)


def test_seed_start_triggers_multiple():
    a, b = _node("a"), _node("b")
    workflow = Workflow(name="wf", edges=[Edge(START, a), Edge(START, b)])
    buffer: dict[str, list[Trigger]] = {}
    workflow._seed_start_triggers(buffer)  # pyright: ignore[reportPrivateUsage]
    assert "a" in buffer
    assert "b" in buffer
