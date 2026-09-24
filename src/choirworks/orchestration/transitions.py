from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from choirworks.orchestration.state import (
    InterventionDelta,
    NodeDelta,
    NodeState,
    NodeStatus,
)

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext


class InvalidTransition(RuntimeError):
    """A node status change the transition table does not allow."""


ALLOWED_TRANSITIONS: dict[NodeStatus, frozenset[NodeStatus]] = {
    NodeStatus.PENDING: frozenset(
        {
            NodeStatus.READY,
            NodeStatus.SUBMITTED,
            NodeStatus.INPUT_REQUIRED,
            NodeStatus.FAILED,
            NodeStatus.CANCELED,
            NodeStatus.INVALIDATED,
        }
    ),
    NodeStatus.READY: frozenset(
        {
            NodeStatus.SUBMITTED,
            NodeStatus.INPUT_REQUIRED,
            NodeStatus.FAILED,
            NodeStatus.CANCELED,
            NodeStatus.INVALIDATED,
        }
    ),
    NodeStatus.SUBMITTED: frozenset(
        {
            NodeStatus.WORKING,
            NodeStatus.COMPLETED,
            NodeStatus.FAILED,
            NodeStatus.INPUT_REQUIRED,
            NodeStatus.CANCELED,
            NodeStatus.RECOVER,
            NodeStatus.PENDING,
            NodeStatus.INVALIDATED,
        }
    ),
    NodeStatus.WORKING: frozenset(
        {
            NodeStatus.SUBMITTED,
            NodeStatus.COMPLETED,
            NodeStatus.FAILED,
            NodeStatus.INPUT_REQUIRED,
            NodeStatus.CANCELED,
            NodeStatus.RECOVER,
            NodeStatus.PENDING,
            NodeStatus.INVALIDATED,
        }
    ),
    NodeStatus.RECOVER: frozenset(
        {
            NodeStatus.SUBMITTED,
            NodeStatus.WORKING,
            NodeStatus.COMPLETED,
            NodeStatus.FAILED,
            NodeStatus.INPUT_REQUIRED,
            NodeStatus.CANCELED,
            NodeStatus.PENDING,
            NodeStatus.INVALIDATED,
        }
    ),
    NodeStatus.INPUT_REQUIRED: frozenset(
        {
            NodeStatus.READY,
            NodeStatus.SUBMITTED,
            NodeStatus.FAILED,
            NodeStatus.CANCELED,
            NodeStatus.INVALIDATED,
        }
    ),
    NodeStatus.FAILED: frozenset({NodeStatus.PENDING, NodeStatus.INVALIDATED}),
    NodeStatus.COMPLETED: frozenset(),
    NodeStatus.CANCELED: frozenset(),
    NodeStatus.INVALIDATED: frozenset(),
}


def can_transition(node: NodeState, to: NodeStatus) -> bool:
    """Whether ``node`` may move to ``to`` (same-status re-assertions allowed)."""
    if to == node.status:
        return True
    return to in ALLOWED_TRANSITIONS.get(node.status, frozenset())


def can_retry(node: NodeState, max_attempts: int) -> bool:
    """Whether a failed node still has attempts left."""
    return node.status == NodeStatus.FAILED and node.attempt < max_attempts


def apply_transition(node: NodeState, to: NodeStatus) -> None:
    """Validate and apply a bare status change."""
    if not can_transition(node, to):
        raise InvalidTransition(f"node {node.id}: {node.status} -> {to}")
    node.status = to


async def transition(
    ctx: OrchestrationContext,
    node: NodeState,
    to: NodeStatus,
    *,
    emit: bool = True,
    delta: Mapping[str, object] | None = None,
    interventions: dict[str, InterventionDelta] | None = None,
) -> None:
    """The single node-status change entry: validate, apply, emit."""
    from choirworks.orchestration.events import emit_state_delta

    apply_transition(node, to)
    node_delta = NodeDelta(status=to)
    if delta is not None:
        node_delta = cast("NodeDelta", cast("object", {**delta, "status": to}))
    if emit:
        await emit_state_delta(
            ctx,
            nodes={node.id: node_delta},
            interventions=interventions,
        )
