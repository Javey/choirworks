from __future__ import annotations

from choirworks.a2a.context import OrchestrationContext
from choirworks.a2a.events import emit_state_delta
from choirworks.a2a.room import RoomOptions
from choirworks.a2a.state import (
    ACTIVE_NODE_STATUSES,
    NodeState,
    active_nodes,
    blocked_nodes,
    enqueue,
)


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
                if node.id == quote_id
                or f"{ctx.task_id}:{node.id}" == quote_id
            ),
            None,
        )
        if quoted_node is not None and quoted_node.status in ACTIVE_NODE_STATUSES:
            enqueue(state,
                quoted_node.id, text, sender="user", quote_id=str(quote_id)
            )
            await ctx.sessions.persist(ctx)
            return
        if quoted_node is not None and quoted_node.status == "completed":
            await spawn_followup_node(ctx, text, quoted_node)
            return

    if interrupt and active:
        node = active[0]
        if node.a2a_task_id:
            await ctx.remote.cancel_task(node.agent_url, node.a2a_task_id)
        node.status = "canceled"
        invalidated = blocked_nodes(state)
        for blocked in invalidated:
            blocked.status = "invalidated"
        await emit_state_delta(ctx, nodes={
            node.id: {"status": "canceled", "agent_name": node.agent_name},
            **{b.id: {"status": "invalidated"} for b in invalidated},
        })
        await spawn_followup_node(ctx, text, node, deps=[])
        return

    target = active[0] if active else next(
        (n for n in state.nodes.values() if n.status in {"pending", "ready"}),
        None,
    )
    if target is not None:
        enqueue(state,
            target.id,
            text,
            sender="user",
            quote_id=str(quote_id) if quote_id else None,
        )
        await ctx.sessions.persist(ctx)
        return

    from choirworks.a2a.planning import plan_and_launch
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
        return
    state.derived_count += 1
    node_id = f"{anchor.id}-f{state.derived_count}"
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
    await emit_state_delta(ctx, nodes={
        node_id: {
            "status": "pending",
            "agent_name": followup.agent_name,
        },
    })
    await ctx.sessions.persist(ctx)
    ctx.runtime.runner_start_requested = True
