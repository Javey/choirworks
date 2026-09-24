from __future__ import annotations

import structlog

from choirworks.a2a.room import RoomOptions
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    NodeState,
    NodeStatus,
    active_nodes,
    blocked_nodes,
    enqueue,
)
from choirworks.orchestration.transitions import apply_transition

logger = structlog.get_logger(__name__)


async def route_message(
    ctx: OrchestrationContext,
    text: str,
    room: RoomOptions,
) -> None:
    """Route a user message: queued follow-up, quote, interrupt, or new plan."""
    state = ctx.state
    if not text:
        return
    active = active_nodes(state)
    quote_id = room.get("quote_id")
    interrupt = bool(room.get("interrupt"))

    if quote_id and not interrupt:
        quoted_node = next(
            (
                node
                for node in state.nodes.values()
                if node.id == quote_id or f"{ctx.task_id}:{node.id}" == quote_id
            ),
            None,
        )
        if quoted_node is not None and quoted_node.status in ACTIVE_NODE_STATUSES:
            logger.info(
                "route_message route=enqueue_active",
                task_id=ctx.task_id,
                node_id=quoted_node.id,
            )
            enqueue(state, quoted_node.id, text, sender="user", quote_id=str(quote_id))
            await ctx.sessions.persist(ctx)
            return
        if quoted_node is not None and quoted_node.status == NodeStatus.COMPLETED:
            logger.info(
                "route_message route=followup",
                task_id=ctx.task_id,
                node_id=quoted_node.id,
            )
            await spawn_followup_node(ctx, text, quoted_node)
            return

    if interrupt and active:
        node = active[0]
        logger.info(
            "route_message route=interrupt",
            task_id=ctx.task_id,
            node_id=node.id,
        )
        if node.a2a_task_id:
            await ctx.remote.cancel_task(node.agent_url, node.a2a_task_id)
        apply_transition(node, NodeStatus.CANCELED)
        invalidated = blocked_nodes(state)
        for blocked in invalidated:
            apply_transition(blocked, NodeStatus.INVALIDATED)
        await emit_state_delta(
            ctx,
            nodes={
                node.id: {"status": NodeStatus.CANCELED, "agent_name": node.agent_name},
                **{b.id: {"status": NodeStatus.INVALIDATED} for b in invalidated},
            },
        )
        await spawn_followup_node(ctx, text, node, deps=[])
        return

    target = (
        active[0]
        if active
        else next(
            (n for n in state.nodes.values() if n.status in {NodeStatus.PENDING, NodeStatus.READY}),
            None,
        )
    )
    if target is not None:
        logger.info(
            "route_message route=enqueue",
            task_id=ctx.task_id,
            node_id=target.id,
        )
        enqueue(
            state,
            target.id,
            text,
            sender="user",
            quote_id=str(quote_id) if quote_id else None,
        )
        await ctx.sessions.persist(ctx)
        return

    from choirworks.orchestration.planning import plan_and_launch

    logger.info(
        "route_message route=new_plan",
        task_id=ctx.task_id,
    )
    await plan_and_launch(ctx, text)


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
