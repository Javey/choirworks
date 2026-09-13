from __future__ import annotations

from enum import StrEnum


class TaskStatus(StrEnum):
    PENDING = "pending"
    PLANNING = "planning"
    RUNNING = "running"
    AWAITING_INPUT = "awaiting_input"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class NodeStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    DISPATCHED = "dispatched"
    WORKING = "working"
    INPUT_REQUIRED = "input_required"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    INVALIDATED = "invalidated"


class InterventionStatus(StrEnum):
    PENDING = "pending"
    RESOLVED = "resolved"
    EXPIRED = "expired"
    FAILED = "failed"
    INVALIDATED = "invalidated"


class EventType(StrEnum):
    TASK_CREATED = "task.created"
    TASK_STATE_CHANGED = "task.state_changed"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"

    PLAN_CREATED = "plan.created"
    PLAN_EXTENDED = "plan.extended"
    PLAN_SUPERSEDED = "plan.superseded"

    NODE_DISPATCH_INTENT = "node.dispatch.intent"
    NODE_DISPATCHED = "node.dispatched"
    NODE_STATE_CHANGED = "node.state_changed"
    NODE_ARTIFACT = "node.artifact"
    NODE_OUTPUT = "node.output"
    NODE_RETRY_SCHEDULED = "node.retry.scheduled"
    NODE_INVALIDATED = "node.invalidated"
    NODE_CANCEL_SENT = "node.cancel.sent"

    INTERVENTION_REQUESTED = "intervention.requested"
    INTERVENTION_RESOLVED = "intervention.resolved"
    INTERVENTION_FAILED = "intervention.failed"

    CHECKPOINT_CREATED = "checkpoint.created"
    ROLLBACK_PERFORMED = "rollback.performed"

    ERROR = "error"


TERMINAL_TASK_STATUSES = {
    TaskStatus.COMPLETED,
    TaskStatus.FAILED,
    TaskStatus.CANCELED,
}

TERMINAL_NODE_STATUSES = {
    NodeStatus.COMPLETED,
    NodeStatus.FAILED,
    NodeStatus.CANCELED,
    NodeStatus.INVALIDATED,
}
