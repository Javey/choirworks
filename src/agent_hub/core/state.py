from __future__ import annotations

from agent_hub.models.enums import NodeStatus, TaskStatus


class InvalidTransition(RuntimeError):
    pass


_TASK_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING: {TaskStatus.PLANNING},
    TaskStatus.PLANNING: {
        TaskStatus.RUNNING,
        TaskStatus.AWAITING_INPUT,
        TaskStatus.FAILED,
        TaskStatus.CANCELED,
    },
    TaskStatus.RUNNING: {
        TaskStatus.AWAITING_INPUT,
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.CANCELED,
    },
    TaskStatus.AWAITING_INPUT: {
        TaskStatus.PLANNING,
        TaskStatus.RUNNING,
        TaskStatus.FAILED,
        TaskStatus.CANCELED,
    },
    TaskStatus.COMPLETED: set(),
    TaskStatus.FAILED: set(),
    TaskStatus.CANCELED: set(),
}

_NODE_TRANSITIONS: dict[NodeStatus, set[NodeStatus]] = {
    NodeStatus.PENDING: {NodeStatus.READY, NodeStatus.INVALIDATED},
    NodeStatus.READY: {
        NodeStatus.DISPATCHED,
        NodeStatus.FAILED,
        NodeStatus.INVALIDATED,
    },
    NodeStatus.DISPATCHED: {
        NodeStatus.WORKING,
        NodeStatus.FAILED,
        NodeStatus.COMPLETED,
        NodeStatus.INPUT_REQUIRED,
        NodeStatus.INVALIDATED,
    },
    NodeStatus.WORKING: {
        NodeStatus.INPUT_REQUIRED,
        NodeStatus.COMPLETED,
        NodeStatus.FAILED,
        NodeStatus.INVALIDATED,
    },
    NodeStatus.INPUT_REQUIRED: {
        NodeStatus.WORKING,
        NodeStatus.FAILED,
        NodeStatus.INVALIDATED,
    },
    NodeStatus.COMPLETED: {NodeStatus.INVALIDATED},
    NodeStatus.FAILED: {NodeStatus.READY, NodeStatus.INVALIDATED},
    NodeStatus.CANCELED: {NodeStatus.INVALIDATED},
    NodeStatus.INVALIDATED: set(),
}


def can_task_transition(source: TaskStatus, target: TaskStatus) -> bool:
    return target in _TASK_TRANSITIONS[source]


def assert_task_transition(source: TaskStatus, target: TaskStatus) -> None:
    if not can_task_transition(source, target):
        raise InvalidTransition(f"illegal task transition: {source.value} -> {target.value}")


def can_node_transition(source: NodeStatus, target: NodeStatus) -> bool:
    return target in _NODE_TRANSITIONS[source]


def assert_node_transition(source: NodeStatus, target: NodeStatus) -> None:
    if not can_node_transition(source, target):
        raise InvalidTransition(f"illegal node transition: {source.value} -> {target.value}")
