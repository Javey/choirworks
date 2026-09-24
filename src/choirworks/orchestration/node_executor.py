from __future__ import annotations

import asyncio

import structlog

from choirworks.core.context import build_continuation_text, build_dispatch_text
from choirworks.orchestration.assist import arbitrate_mentions
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_pending_questions, emit_state_delta
from choirworks.orchestration.remote_caller import recover_remote, stream_remote
from choirworks.orchestration.repair import revise_plan
from choirworks.orchestration.routing import spawn_followup_node
from choirworks.orchestration.state import (
    NodeState,
    NodeStatus,
    expire_cancel_requests,
    take_queued,
)
from choirworks.subagents import OUTCOME_SUBAGENT, run_subagent
from choirworks.tools.outcome_decision import OutcomeDecision

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
        await emit_state_delta(
            ctx,
            nodes={
                node.id: {"status": NodeStatus.SUBMITTED, "input_text": node.input_text},
            },
        )
    elif mode == "recover":
        await emit_state_delta(
            ctx,
            nodes={
                node.id: {"status": "recover", "a2a_task_id": node.a2a_task_id},
            },
        )
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
        node.status = NodeStatus.FAILED
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        node.error = str(exc)
        node.status = NodeStatus.FAILED

    if current == NodeStatus.COMPLETED:
        await _handle_completed(ctx, node)
    elif current == NodeStatus.CANCELED:
        logger.info("execute_node canceled", context_id=ctx.context_id, node_id=node.id)
        node.status = NodeStatus.CANCELED
        await emit_state_delta(
            ctx,
            nodes={
                node.id: {"status": NodeStatus.CANCELED},
            },
        )
    elif current == NodeStatus.INPUT_REQUIRED:
        logger.info("execute_node input_required", context_id=ctx.context_id, node_id=node.id)
        node.status = NodeStatus.INPUT_REQUIRED
        await emit_state_delta(
            ctx,
            nodes={
                node.id: {
                    "status": NodeStatus.INPUT_REQUIRED,
                    "question": node.question or "",
                    "agent_name": node.agent_name,
                },
            },
        )
    else:
        node.status = NodeStatus.FAILED
        logger.warning(
            "Node failed",
            context_id=ctx.context_id,
            node_id=node.id,
            agent=node.agent_name,
            error=node.error,
        )
        await emit_state_delta(
            ctx,
            nodes={
                node.id: {"status": NodeStatus.FAILED, "error": node.error or "unknown error"},
            },
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


async def _handle_completed(ctx: OrchestrationContext, node: NodeState) -> None:
    decision = await _interpret_outcome(ctx, node)
    logger.info(
        "handle_completed",
        context_id=ctx.context_id,
        node_id=node.id,
        intent=decision.intent,
    )
    if decision.intent == "need_info":
        node.status = NodeStatus.INPUT_REQUIRED
        node.question = decision.question or node.output
        node.a2a_task_id = None
        await emit_state_delta(
            ctx,
            nodes={
                node.id: {
                    "status": NodeStatus.INPUT_REQUIRED,
                    "question": node.question or "",
                    "agent_name": node.agent_name,
                },
            },
        )
    else:
        if decision.intent == "revise" and decision.patch is not None:
            if ctx.state.revision_count < ctx.config.max_revisions:
                async with ctx.lock:
                    await revise_plan(ctx, decision.patch)
            else:
                logger.warning(
                    "Revision limit reached, skipping",
                    context_id=ctx.context_id,
                )
        node.status = NodeStatus.COMPLETED
        await emit_state_delta(
            ctx,
            nodes={
                node.id: {
                    "status": NodeStatus.COMPLETED,
                    "agent_name": node.agent_name,
                    "output": (node.output or "")[:200],
                },
            },
        )
        await arbitrate_mentions(ctx, node)
        await _deliver_queued(ctx, node)


async def _interpret_outcome(ctx: OrchestrationContext, node: NodeState) -> OutcomeDecision:
    from choirworks.orchestration.markers import parse_marker

    marker = parse_marker(node.output)
    if marker is not None:
        return OutcomeDecision(intent=marker.intent, question=marker.text)
    if not node.output:
        return OutcomeDecision(intent="deliver")
    agents = await ctx.registry.list()
    candidates = [agent for agent in agents if agent.name != node.agent_name]
    from choirworks.core.context import build_outcome_user

    user = build_outcome_user(
        node.agent_name,
        node.input_text,
        node.output,
        candidates,
    )
    try:
        return await run_subagent(
            OUTCOME_SUBAGENT,
            ctx,
            user,
            exclude_agent=node.agent_name,
        )
    except Exception:
        logger.exception(
            "outcome interpretation failed", context_id=ctx.context_id, node_id=node.id
        )
        return OutcomeDecision(intent="deliver")


async def _deliver_queued(ctx: OrchestrationContext, node: NodeState) -> None:
    messages = take_queued(ctx.state, node.id)
    if not messages:
        return
    text = "\n\n".join(message.text for message in messages)
    await spawn_followup_node(ctx, text, node, deps=[node.id])
