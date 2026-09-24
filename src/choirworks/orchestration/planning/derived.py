from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING

import structlog

from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.helpers import join_members
from choirworks.orchestration.state import NodeState, NodeStatus

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

logger = structlog.get_logger(__name__)


class DerivedKind(StrEnum):
    FOLLOWUP = "followup"
    ASSIST = "assist"
    HELPER = "helper"


_SUFFIX: dict[DerivedKind, str] = {
    DerivedKind.FOLLOWUP: "f",
    DerivedKind.ASSIST: "a",
    DerivedKind.HELPER: "h",
}
_JOIN_REASON: dict[DerivedKind, str] = {
    DerivedKind.ASSIST: "agent_mention",
    DerivedKind.HELPER: "peer_assist",
}


async def spawn_derived_node(
    ctx: OrchestrationContext,
    kind: DerivedKind,
    *,
    parent_id: str,
    agent_name: str,
    agent_url: str,
    input_text: str,
    deps: list[str] | None = None,
    assist_requested_by: str | None = None,
    source_message_id: str | None = None,
    emit: bool = True,
) -> NodeState | None:
    """Create a derived node, join its agent, persist, and (optionally) emit.

    ``kind`` selects the id suffix and join reason; follow-ups do not join a
    member.  Returns ``None`` when the derived-node budget is exhausted.
    """
    state = ctx.state
    if state.derived_count >= ctx.config.max_derived_nodes:
        logger.info(
            "spawn_derived_node max_derived reached",
            kind=kind,
            parent_id=parent_id,
            agent_name=agent_name,
        )
        return None
    state.derived_count += 1
    node_id = f"{parent_id}-{_SUFFIX[kind]}{state.derived_count}"
    node = NodeState(
        id=node_id,
        name="",
        agent_name=agent_name,
        agent_url=agent_url,
        deps=list(deps or []),
        input_text=input_text,
        derived=True,
        assist_requested_by=assist_requested_by,
        source_message_id=source_message_id,
    )
    state.nodes[node_id] = node
    logger.info("spawn_derived_node", kind=kind, node_id=node_id, agent_name=agent_name)
    if kind is not DerivedKind.FOLLOWUP:
        await join_members(ctx, [agent_name], _JOIN_REASON[kind])
    if emit:
        await emit_state_delta(
            ctx,
            nodes={
                node_id: {
                    "status": NodeStatus.PENDING,
                    "agent_name": agent_name,
                    "input_text": input_text,
                },
            },
        )
    await ctx.sessions.persist(ctx)
    return node


async def spawn_followup_node(
    ctx: OrchestrationContext,
    text: str,
    anchor: NodeState,
    *,
    deps: list[str] | None = None,
) -> None:
    """Create a derived follow-up node handled by the anchor's agent."""
    _ = await spawn_derived_node(
        ctx,
        DerivedKind.FOLLOWUP,
        parent_id=anchor.id,
        agent_name=anchor.agent_name,
        agent_url=anchor.agent_url,
        input_text=text,
        deps=deps if deps is not None else [anchor.id],
    )
