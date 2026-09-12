import pytest

from agent_hub.core.state import (
    InvalidTransition,
    assert_node_transition,
    assert_task_transition,
    can_node_transition,
    can_task_transition,
)
from agent_hub.models.enums import NodeStatus, TaskStatus


def test_task_legal_transitions():
    assert can_task_transition(TaskStatus.PENDING, TaskStatus.PLANNING)
    assert can_task_transition(TaskStatus.PLANNING, TaskStatus.RUNNING)
    assert can_task_transition(TaskStatus.RUNNING, TaskStatus.AWAITING_INPUT)
    assert can_task_transition(TaskStatus.AWAITING_INPUT, TaskStatus.RUNNING)
    assert can_task_transition(TaskStatus.RUNNING, TaskStatus.COMPLETED)
    assert can_task_transition(TaskStatus.RUNNING, TaskStatus.FAILED)
    assert can_task_transition(TaskStatus.RUNNING, TaskStatus.CANCELED)


def test_task_illegal_transitions():
    assert not can_task_transition(TaskStatus.PENDING, TaskStatus.RUNNING)
    assert not can_task_transition(TaskStatus.COMPLETED, TaskStatus.RUNNING)
    with pytest.raises(InvalidTransition):
        assert_task_transition(TaskStatus.COMPLETED, TaskStatus.RUNNING)


def test_node_legal_transitions():
    assert can_node_transition(NodeStatus.PENDING, NodeStatus.READY)
    assert can_node_transition(NodeStatus.READY, NodeStatus.DISPATCHED)
    assert can_node_transition(NodeStatus.DISPATCHED, NodeStatus.WORKING)
    assert can_node_transition(NodeStatus.WORKING, NodeStatus.INPUT_REQUIRED)
    assert can_node_transition(NodeStatus.INPUT_REQUIRED, NodeStatus.WORKING)
    assert can_node_transition(NodeStatus.WORKING, NodeStatus.COMPLETED)
    assert can_node_transition(NodeStatus.READY, NodeStatus.FAILED)
    assert can_node_transition(NodeStatus.FAILED, NodeStatus.READY)


def test_node_invalidated_from_every_non_terminal_state():
    for status in [
        NodeStatus.PENDING,
        NodeStatus.READY,
        NodeStatus.DISPATCHED,
        NodeStatus.WORKING,
        NodeStatus.INPUT_REQUIRED,
        NodeStatus.COMPLETED,
        NodeStatus.FAILED,
        NodeStatus.CANCELED,
    ]:
        assert can_node_transition(status, NodeStatus.INVALIDATED)


def test_node_cannot_leave_invalidated():
    with pytest.raises(InvalidTransition):
        assert_node_transition(NodeStatus.INVALIDATED, NodeStatus.READY)
