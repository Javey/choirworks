from __future__ import annotations

import asyncio

import structlog

from choirworks.core.context import build_continuation_text, build_dispatch_text
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_pending_questions, emit_state_delta
from choirworks.orchestration.execution.remote_caller import recover_remote, stream_remote
from choirworks.orchestration.outcome import OutcomePayload, outcome_flow
from choirworks.orchestration.state import (
    NodeState,
    NodeStatus,
    expire_cancel_requests,
)
from choirworks.orchestration.transitions import transition

logger = structlog.get_logger(__name__)


async def build_node_text(ctx: OrchestrationContext, node: NodeState) -> list[str]:
    agents = await ctx.registry.list()
    by_name = {agent.name: agent for agent in agents}
    if node.answer_text is not None:
        text = build_continuation_text(
            node,
            ctx.state,
            by_name,
            question=node.question or "",
            answer=node.answer_text,
        )
        node.answer_text = None
        node.question = None
        return text
    return build_dispatch_text(node, ctx.state, by_name)


async def execute_node(
    ctx: OrchestrationContext,
    node: NodeState,
    *,
    mode: str = "dispatch",
) -> None:
    """Single node execution: dispatch → stream → interpret → act."""
    continuation = mode == "continue"
    if mode != "recover":
        node.attempt += 1
    logger.info(
        "execute_node",
        context_id=ctx.context_id,
        node_id=node.id,
        agent=node.agent_name,
        mode=mode,
        attempt=node.attempt,
    )
    if mode == "dispatch":
        await transition(ctx, node, NodeStatus.SUBMITTED, delta={"input_text": node.input_text})
    elif mode == "recover":
        await transition(ctx, node, NodeStatus.RECOVER, delta={"a2a_task_id": node.a2a_task_id})
    current = "working"
    try:
        async with asyncio.timeout(ctx.config.node_timeout):
            if mode == "recover":
                current = await recover_remote(ctx, node)
            else:
                text = await build_node_text(ctx, node)
                current = await stream_remote(ctx, node, text, continuation=continuation)
    except TimeoutError:
        node.error = f"node timed out after {ctx.config.node_timeout}s"
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        node.error = str(exc)

    if current == NodeStatus.COMPLETED:
        _ = await outcome_flow.run(ctx, OutcomePayload(node=node))
    elif current == NodeStatus.CANCELED:
        logger.info("execute_node canceled", context_id=ctx.context_id, node_id=node.id)
        await transition(ctx, node, NodeStatus.CANCELED)
    elif current == NodeStatus.INPUT_REQUIRED:
        logger.info("execute_node input_required", context_id=ctx.context_id, node_id=node.id)
        await transition(
            ctx,
            node,
            NodeStatus.INPUT_REQUIRED,
            delta={"question": node.question or "", "agent_name": node.agent_name},
        )
    else:
        logger.warning(
            "Node failed",
            context_id=ctx.context_id,
            node_id=node.id,
            agent=node.agent_name,
            error=node.error,
        )
        await transition(
            ctx,
            node,
            NodeStatus.FAILED,
            delta={"error": node.error or "unknown error"},
        )

    expired = expire_cancel_requests(ctx.state, node.id)
    if expired:
        await emit_state_delta(
            ctx,
            interventions={
                iv.id: {
                    "status": "expired",
                    "node_id": node.id,
                    "kind": "confirm_cancel",
                }
                for iv in expired
            },
        )
    await ctx.sessions.persist(ctx)
    await emit_pending_questions(ctx)
