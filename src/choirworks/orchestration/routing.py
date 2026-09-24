from __future__ import annotations

import structlog

from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.state import NodeState, NodeStatus

logger = structlog.get_logger(__name__)


async def spawn_followup_node(
    ctx: OrchestrationContext,
    text: str,
    anchor: NodeState,
    *,
    deps: list[str] | None = None,
) -> None:
    """Create a derived follow-up node, emit state, persist, flag runner."""
    state = ctx.state
    if state.derived_count >= ctx.config.max_derived_nodes:
        logger.info(
            "spawn_followup_node max_derived reached, skipping",
            anchor_id=anchor.id,
        )
        return
    state.derived_count += 1
    node_id = f"{anchor.id}-f{state.derived_count}"
    logger.info(
        "spawn_followup_node",
        node_id=node_id,
        agent=anchor.agent_name,
        anchor_id=anchor.id,
    )
    followup = NodeState(
        id=node_id,
        name="",
        agent_name=anchor.agent_name,
        agent_url=anchor.agent_url,
        deps=list(deps if deps is not None else [anchor.id]),
        input_text=text,
        derived=True,
    )
    state.nodes[node_id] = followup
    await emit_state_delta(
        ctx,
        nodes={
            node_id: {
                "status": NodeStatus.PENDING,
                "agent_name": followup.agent_name,
                "input_text": followup.input_text,
            },
        },
    )
    await ctx.sessions.persist(ctx)
