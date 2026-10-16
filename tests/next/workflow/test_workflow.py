from new_choirworks.core.workflow.base_node import START, BaseNode
from new_choirworks.core.workflow.graph import Edge
from new_choirworks.core.workflow.trigger import Trigger
from new_choirworks.core.workflow.workflow import Workflow, _LoopState  # pyright: ignore[reportPrivateUsage]


def _node(name: str) -> BaseNode:
    return BaseNode(name=name)


def test_seed_start_triggers_single():
    a = _node("a")
    workflow = Workflow(name="wf", edges=[Edge(START, a)])
    loop_state = _LoopState(completed={START.name})
    workflow._seed_start_triggers(loop_state, None)  # pyright: ignore[reportPrivateUsage]
    assert "a" in loop_state.trigger_buffer
    assert len(loop_state.trigger_buffer["a"]) == 1
    assert isinstance(loop_state.trigger_buffer["a"][0], Trigger)


def test_seed_start_triggers_multiple():
    a, b = _node("a"), _node("b")
    workflow = Workflow(name="wf", edges=[Edge(START, a), Edge(START, b)])
    loop_state = _LoopState(completed={START.name})
    workflow._seed_start_triggers(loop_state, None)  # pyright: ignore[reportPrivateUsage]
    assert "a" in loop_state.trigger_buffer
    assert "b" in loop_state.trigger_buffer
