from __future__ import annotations

import structlog
from a2a.types.a2a_pb2 import TaskState

from choirworks.core.context import build_assistance_decision_user
from choirworks.orchestration.assist import spawn_assist
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.flows import execute_function
from choirworks.orchestration.remote_caller import cancel_remote_task
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    PENDING_NODE_STATUSES,
    InterventionStatus,
    NodeState,
    NodeStatus,
    add_intervention,
    blocked_nodes,
    input_required_nodes,
    pending_intervention_for,
    pending_interventions,
)
from choirworks.subagents import ASSISTANCE_SUBAGENT, run_subagent
from choirworks.tools import ask_user_func
from choirworks.tools.ask_user import AskUserArgs
from choirworks.tools.outcome_decision import OutcomeDecision

logger = structlog.get_logger(__name__)

_AFFIRMATIVE_ANSWERS = {"确认", "确定", "打断", "是", "yes", "y", "ok"}


def _is_affirmative(text: str) -> bool:
    return text.strip().lower() in _AFFIRMATIVE_ANSWERS


async def settle_input(ctx: OrchestrationContext) -> bool:
    """Resolve ``input_required`` nodes: peer assistance or a human question."""
    state = ctx.state
    progress = False
    for node in list(input_required_nodes(state)):
        intervention = pending_intervention_for(state, node.id)
        if intervention is not None:
            continue
        helpers = [
            n
            for n in state.nodes.values()
            if n.derived and n.assist_requested_by == node.id and n.status == NodeStatus.COMPLETED
        ]
        if helpers:
            helper = helpers[0]
            intervention = pending_intervention_for(state, node.id)
            if intervention is None:
                intervention = add_intervention(state, node.id, node.question or "")
            intervention.status = InterventionStatus.RESOLVED
            intervention.answer = helper.output
            intervention.responder = helper.id
            node.answer_text = helper.output
            node.status = NodeStatus.READY
            await emit_state_delta(
                ctx,
                nodes={node.id: {"status": NodeStatus.READY}},
                interventions={
                    intervention.id: {
                        "status": "resolved",
                        "node_id": node.id,
                        "kind": intervention.kind,
                        "responder": helper.id,
                    },
                },
            )
            progress = True
            continue
        active_helpers = [
            n
            for n in state.nodes.values()
            if n.derived
            and n.assist_requested_by == node.id
            and n.status in ACTIVE_NODE_STATUSES | PENDING_NODE_STATUSES
        ]
        if active_helpers:
            continue

        decision = await _decide_assistance(ctx, node)
        if decision is not None and decision.target_agent:
            if await spawn_assist(ctx, node, decision):
                progress = True
            else:
                await request_human(ctx, node)
        else:
            await request_human(ctx, node)
    return progress


async def _decide_assistance(ctx: OrchestrationContext, node: NodeState) -> OutcomeDecision | None:
    if node.question is None:
        return None
    agents = await ctx.registry.list()
    candidates = [agent for agent in agents if agent.name != node.agent_name]
    if not candidates:
        return OutcomeDecision(intent="need_info")
    user = build_assistance_decision_user(
        node.agent_name,
        node.question or node.input_text,
        candidates,
    )
    try:
        return await run_subagent(
            ASSISTANCE_SUBAGENT,
            ctx,
            user,
            exclude_agent=node.agent_name,
        )
    except Exception:
        logger.exception("assistance decision failed", node=node.id)
        return OutcomeDecision(intent="need_info")


async def request_human(ctx: OrchestrationContext, node: NodeState) -> None:
    logger.info(
        "request_human",
        node=node.id,
        agent=node.agent_name,
        question_len=len(node.question or node.output or ""),
    )
    args = AskUserArgs(node_id=node.id, question=node.question or node.output or "")
    await execute_function(
        ctx,
        ask_user_func,
        args,
        state_name=TaskState.TASK_STATE_INPUT_REQUIRED,
    )


async def answer_intervention(
    ctx: OrchestrationContext,
    text: str,
) -> None:
    state = ctx.state
    pending = pending_interventions(state)
    if not pending or not text:
        return
    intervention = pending[0]
    intervention.status = InterventionStatus.RESOLVED
    intervention.answer = text
    intervention.responder = "human"
    logger.info(
        "answer_intervention",
        id=intervention.id,
        kind=intervention.kind,
        affirmative=_is_affirmative(text),
    )
    if intervention.kind == "confirm_cancel":
        target = state.nodes.get(intervention.target_node_id or "")
        if target is not None and _is_affirmative(text):
            await cancel_node(ctx, target)
        await emit_state_delta(
            ctx,
            interventions={
                intervention.id: {
                    "status": "resolved",
                    "node_id": intervention.node_id,
                    "kind": "confirm_cancel",
                    "responder": "human",
                },
            },
        )
        await ctx.sessions.persist(ctx)
        ctx.runtime.runner_start_requested = True
        return
    node = state.nodes.get(intervention.node_id)
    if node is not None:
        node.answer_text = text
        node.status = NodeStatus.READY
    await emit_state_delta(
        ctx,
        nodes={node.id: {"status": NodeStatus.READY}} if node else None,
        interventions={
            intervention.id: {
                "status": "resolved",
                "node_id": intervention.node_id,
                "kind": intervention.kind,
                "responder": "human",
            },
        },
    )
    ctx.runtime.runner_start_requested = True


async def cancel_node(
    ctx: OrchestrationContext,
    node: NodeState,
) -> None:
    logger.info(
        "cancel_node",
        node=node.id,
        invalidated=[n.id for n in blocked_nodes(ctx.state)],
    )
    state = ctx.state
    if node.a2a_task_id and node.agent_url:
        await cancel_remote_task(ctx, node.agent_url, node.a2a_task_id)
    node.status = NodeStatus.CANCELED
    node.a2a_task_id = None
    invalidated = blocked_nodes(state)
    for blocked in invalidated:
        blocked.status = NodeStatus.INVALIDATED
    await emit_state_delta(
        ctx,
        nodes={
            node.id: {"status": NodeStatus.CANCELED, "agent_name": node.agent_name},
            **{b.id: {"status": NodeStatus.INVALIDATED} for b in invalidated},
        },
    )
    await ctx.sessions.persist(ctx)
