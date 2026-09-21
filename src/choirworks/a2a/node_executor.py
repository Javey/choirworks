from __future__ import annotations

import asyncio
import logging

from a2a.types.a2a_pb2 import TaskState

from choirworks.a2a.assist import arbitrate_mentions
from choirworks.a2a.context import OrchestrationContext
from choirworks.a2a.events import emit_state_delta
from choirworks.a2a.remote_caller import resume_remote, stream_remote
from choirworks.a2a.repair import revise_plan
from choirworks.a2a.state import NodeState, expire_cancel_requests, take_queued
from choirworks.core.context import build_continuation_text, build_dispatch_text
from choirworks.subagents import OUTCOME_SUBAGENT, run_subagent
from choirworks.tools.outcome_decision import OutcomeDecision

logger = logging.getLogger(__name__)


async def build_node_text(ctx: OrchestrationContext, node: NodeState) -> str:
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
    if mode != "resume":
        node.attempt += 1
    if mode == "dispatch":
        await emit_state_delta(ctx, nodes={
            node.id: {"status": "dispatched"},
        })
    elif mode == "resume":
        await emit_state_delta(ctx, nodes={
            node.id: {"status": "resume", "a2a_task_id": node.a2a_task_id},
        })
    current = "working"
    try:
        async with asyncio.timeout(ctx.config.node_timeout):
            if mode == "resume":
                current = await resume_remote(ctx, node)
            else:
                text = await build_node_text(ctx, node)
                current = await stream_remote(
                    ctx, node, text, continuation=continuation
                )
    except TimeoutError:
        node.error = f"node timed out after {ctx.config.node_timeout}s"
        node.status = "failed"
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        node.error = str(exc)
        node.status = "failed"

    if current == "completed":
        await _handle_completed(ctx, node)
    elif current == "canceled":
        node.status = "canceled"
        await emit_state_delta(ctx, nodes={
            node.id: {"status": "canceled"},
        })
    elif current == "input_required":
        node.status = "input_required"
        await emit_state_delta(
            ctx,
            nodes={node.id: {
                "status": "input_required",
                "question": node.question or "",
                "agent_name": node.agent_name,
            }},
            state_name=TaskState.TASK_STATE_INPUT_REQUIRED,
        )
    else:
        node.status = "failed"
        logger.warning(
            "Node %s (%s) failed: %s", node.id, node.agent_name, node.error
        )
        await emit_state_delta(ctx, nodes={
            node.id: {"status": "failed", "error": node.error or "unknown error"},
        })

    expired = expire_cancel_requests(ctx.state, node.id)
    if expired:
        await emit_state_delta(ctx, interventions={
            iv.id: {
                "status": "expired",
                "node_id": node.id,
                "kind": "confirm_cancel",
            }
            for iv in expired
        })
    await ctx.sessions.persist(ctx)


async def _handle_completed(ctx: OrchestrationContext, node: NodeState) -> None:
    decision = await _interpret_outcome(ctx, node)
    if decision.intent == "need_info":
        node.status = "input_required"
        node.question = decision.question or node.output
        node.a2a_task_id = None
        await emit_state_delta(
            ctx,
            nodes={node.id: {
                "status": "input_required",
                "question": node.question or "",
                "agent_name": node.agent_name,
            }},
            state_name=TaskState.TASK_STATE_INPUT_REQUIRED,
        )
    else:
        if decision.intent == "revise" and decision.patch is not None:
            if ctx.state.revision_count < ctx.config.max_revisions:
                async with ctx.lock:
                    await revise_plan(ctx, decision.patch)
            else:
                logger.warning(
                    "Revision limit reached for %s, skipping",
                    ctx.context_id,
                )
        node.status = "completed"
        await emit_state_delta(ctx, nodes={
            node.id: {
                "status": "completed",
                "agent_name": node.agent_name,
                "output": (node.output or "")[:200],
            },
        })
        await arbitrate_mentions(ctx, node)
        await _deliver_queued(ctx, node)


async def _interpret_outcome(
    ctx: OrchestrationContext, node: NodeState
) -> OutcomeDecision:
    from choirworks.a2a.markers import parse_marker
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
            ctx.llm, OUTCOME_SUBAGENT, ctx, user,
            exclude_agent=node.agent_name,
        )
    except Exception:
        logger.exception("outcome interpretation failed for %s", node.id)
        return OutcomeDecision(intent="deliver")


async def _deliver_queued(ctx: OrchestrationContext, node: NodeState) -> None:
    messages = take_queued(ctx.state, node.id)
    if not messages:
        return
    text = "\n\n".join(message.text for message in messages)
    await _spawn_followup_node(ctx, text, node, deps=[node.id])


async def _spawn_followup_node(
    ctx: OrchestrationContext,
    text: str,
    anchor: NodeState,
    *,
    deps: list[str] | None = None,
) -> None:
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
