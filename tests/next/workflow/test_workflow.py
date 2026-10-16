import asyncio

import pytest

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.workflow.base_node import START, BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.graph import Edge
from new_choirworks.core.workflow.node_state import NodeState
from new_choirworks.core.workflow.node_status import NodeStatus
from new_choirworks.core.workflow.trigger import Trigger
from new_choirworks.core.workflow.workflow import (
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
    workflow._seed_start_triggers(loop_state, None)
    assert "a" in loop_state.trigger_buffer
    assert len(loop_state.trigger_buffer["a"]) == 1
    assert isinstance(loop_state.trigger_buffer["a"][0], Trigger)


def test_seed_start_triggers_multiple():
    a, b = _node("a"), _node("b")
    workflow = Workflow(name="wf", edges=[Edge(START, a), Edge(START, b)])
    loop_state = _loop_state()
    workflow._seed_start_triggers(loop_state, None)
    assert "a" in loop_state.trigger_buffer
    assert "b" in loop_state.trigger_buffer


@pytest.mark.asyncio
async def test_cleanup_all_tasks_marks_cancelled():
    workflow = Workflow(name="wf", edges=[Edge(START, _node("a"))])
    loop_state = _loop_state()
    loop_state.nodes["a"] = NodeState(status=NodeStatus.RUNNING)

    ctx = Context(invocation_context=InvocationContext(), node=workflow)

    async def _hang() -> Context:
        await asyncio.sleep(10)
        return ctx

    loop_state.pending_tasks["a"] = asyncio.create_task(_hang())
    await workflow._cleanup_all_tasks(loop_state)

    assert loop_state.nodes["a"].status == NodeStatus.CANCELLED
    assert loop_state.pending_tasks["a"].cancelled()
