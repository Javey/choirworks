from new_choirworks.core.workflow.base_node import START, BaseNode
from new_choirworks.core.workflow.graph import Edge
from new_choirworks.core.workflow.node_state import NodeState
from new_choirworks.core.workflow.node_status import NodeStatus
from new_choirworks.core.workflow.trigger import Trigger
from new_choirworks.core.workflow.workflow import (  # pyright: ignore[reportPrivateUsage]
    Workflow,
    _LoopState,
)


def _node(name: str) -> BaseNode:
    return BaseNode(name=name)


def _loop_state() -> _LoopState:
    ls = _LoopState()
    ls.nodes[START.name] = NodeState(status=NodeStatus.COMPLETED)
    return ls


def test_seed_start_triggers_single():
    a = _node("a")
    workflow = Workflow(name="wf", edges=[Edge(START, a)])
    loop_state = _loop_state()
    workflow._seed_start_triggers(loop_state, None)  # pyright: ignore[reportPrivateUsage]
    assert "a" in loop_state.trigger_buffer
    assert len(loop_state.trigger_buffer["a"]) == 1
    assert isinstance(loop_state.trigger_buffer["a"][0], Trigger)


def test_seed_start_triggers_multiple():
    a, b = _node("a"), _node("b")
    workflow = Workflow(name="wf", edges=[Edge(START, a), Edge(START, b)])
    loop_state = _loop_state()
    workflow._seed_start_triggers(loop_state, None)  # pyright: ignore[reportPrivateUsage]
    assert "a" in loop_state.trigger_buffer
    assert "b" in loop_state.trigger_buffer
