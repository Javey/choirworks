from __future__ import annotations

import structlog

from choirworks.core.context import build_assistance_decision_user
from choirworks.orchestration.assist import spawn_assist
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import (
    emit_intervention_rejected,
    emit_pending_questions,
    emit_state_delta,
)
from choirworks.orchestration.flows import execute_function
from choirworks.orchestration.remote_caller import cancel_remote_task
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    PENDING_NODE_STATUSES,
    Intervention,
    InterventionKind,
    InterventionStatus,
    NodeState,
    NodeStatus,
    QuestionType,
    add_intervention,
    blocked_nodes,
    input_required_nodes,
    pending_intervention_for,
)
from choirworks.subagents import ASSISTANCE_SUBAGENT, run_subagent
from choirworks.tools import ask_user_func
from choirworks.tools.ask_user import AskUserArgs
from choirworks.tools.outcome_decision import OutcomeDecision

logger = structlog.get_logger(__name__)


async def settle_input(ctx: OrchestrationContext) -> bool:
    """Resolve all ``input_required`` nodes (bulk wrapper for tests/recovery)."""
    progress = False
    for node in list(input_required_nodes(ctx.state)):
        if await settle_node_input(ctx, node):
            progress = True
    return progress


async def settle_node_input(ctx: OrchestrationContext, node: NodeState) -> bool:
    """Settle one ``input_required`` node: peer assistance or a human question.

    Mutations run under ``ctx.lock`` so this can execute as a background task
    while the runner keeps dispatching other nodes.
    """
    state = ctx.state
    if pending_intervention_for(state, node.id) is not None:
        return False
    helpers = [
        n
        for n in state.nodes.values()
        if n.derived and n.assist_requested_by == node.id and n.status == NodeStatus.COMPLETED
    ]
    if helpers:
        helper = helpers[0]
        async with ctx.lock:
            intervention = pending_intervention_for(ctx.state, node.id)
            if intervention is None:
                intervention = add_intervention(ctx.state, node.id, node.question or "")
            intervention.status = InterventionStatus.RESOLVED
            intervention.answer = helper.output
            intervention.responder = helper.id
            node.answer_text = helper.output
            node.status = NodeStatus.READY
            await ctx.sessions.persist(ctx)
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
        return True
    active_helpers = [
        n
        for n in state.nodes.values()
        if n.derived
        and n.assist_requested_by == node.id
        and n.status in ACTIVE_NODE_STATUSES | PENDING_NODE_STATUSES
    ]
    if active_helpers:
        return False

    decision = await _decide_assistance(ctx, node)
    async with ctx.lock:
        if pending_intervention_for(ctx.state, node.id) is not None:
            return False
        if decision is not None and decision.target_agent:
            if await spawn_assist(ctx, node, decision):
                return True
        await request_human(ctx, node, decision)
    return False


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


async def request_human(
    ctx: OrchestrationContext,
    node: NodeState,
    decision: OutcomeDecision | None = None,
) -> None:
    logger.info(
        "request_human",
        node=node.id,
        agent=node.agent_name,
        question_len=len(node.question or node.output or ""),
    )
    args = AskUserArgs(
        node_id=node.id,
        question=node.question or node.output or "",
        question_type=decision.question_type if decision else QuestionType.INPUT,
        options=list(decision.options) if decision else [],
        multi=decision.multi if decision else False,
    )
    await execute_function(ctx, ask_user_func, args)
    await ctx.sessions.persist(ctx)
    await emit_pending_questions(ctx)


def render_answer(intervention: Intervention) -> str:
    """Render a typed answer as the continuation text sent to the node."""
    answer = intervention.answer
    if intervention.question_type == QuestionType.SELECT and isinstance(answer, list):
        return "、".join(answer)
    if intervention.question_type == QuestionType.CONFIRM:
        return "确认" if answer is True else "取消"
    return answer if isinstance(answer, str) else ""


def _validate_answer(intervention: Intervention, answer: str | list[str] | bool) -> str | None:
    if intervention.question_type == QuestionType.SELECT:
        options = set(intervention.options)
        if intervention.multi:
            if not isinstance(answer, list) or not answer:
                return "multi-select answer must be a non-empty list"
            if not set(answer) <= options:
                return "answer contains options outside the allowed set"
        elif not isinstance(answer, str) or answer not in options:
            return "answer must be one of the allowed options"
    elif intervention.question_type == QuestionType.CONFIRM:
        if not isinstance(answer, bool):
            return "confirm answer must be a boolean"
    elif not isinstance(answer, str) or not answer.strip():
        return "input answer must be a non-empty string"
    return None


async def _expire(ctx: OrchestrationContext, intervention: Intervention) -> bool:
    intervention.status = InterventionStatus.EXPIRED
    await ctx.sessions.persist(ctx)
    await emit_state_delta(
        ctx,
        interventions={
            intervention.id: {
                "status": "expired",
                "node_id": intervention.node_id,
                "kind": intervention.kind,
            },
        },
    )
    return False


async def answer_intervention(
    ctx: OrchestrationContext,
    *,
    intervention_id: str,
    answer: str | list[str] | bool,
) -> bool:
    """Resolve one pending intervention by id with a typed answer.

    Returns True when the intervention was resolved and the runner should be
    woken.  Unknown / stale ids and ill-typed answers are rejected (the
    intervention expires when its target is gone), never resurrecting nodes.
    """
    state = ctx.state
    intervention = state.interventions.get(intervention_id)
    if intervention is None or intervention.status != InterventionStatus.PENDING:
        await emit_intervention_rejected(ctx, intervention_id, "unknown or resolved intervention")
        return False
    error = _validate_answer(intervention, answer)
    if error is not None:
        await emit_intervention_rejected(ctx, intervention_id, error)
        return False
    logger.info(
        "answer_intervention",
        id=intervention.id,
        kind=intervention.kind,
        question_type=intervention.question_type,
    )
    if intervention.kind == InterventionKind.CONFIRM_CANCEL:
        target = state.nodes.get(intervention.target_node_id or "")
        if target is None or target.status not in ACTIVE_NODE_STATUSES:
            return await _expire(ctx, intervention)
        intervention.status = InterventionStatus.RESOLVED
        intervention.answer = answer
        intervention.responder = "human"
        if answer is True:
            await cancel_node(ctx, target)
        await ctx.sessions.persist(ctx)
        await emit_state_delta(
            ctx,
            interventions={
                intervention.id: {
                    "status": "resolved",
                    "node_id": intervention.node_id,
                    "kind": "confirm_cancel",
                    "responder": "human",
                    "answer": answer,
                },
            },
        )
        return True
    node = state.nodes.get(intervention.node_id)
    if node is None or node.status != NodeStatus.INPUT_REQUIRED:
        return await _expire(ctx, intervention)
    intervention.status = InterventionStatus.RESOLVED
    intervention.answer = answer
    intervention.responder = "human"
    node.answer_text = render_answer(intervention)
    node.status = NodeStatus.READY
    await ctx.sessions.persist(ctx)
    await emit_state_delta(
        ctx,
        nodes={node.id: {"status": NodeStatus.READY}},
        interventions={
            intervention.id: {
                "status": "resolved",
                "node_id": intervention.node_id,
                "kind": intervention.kind,
                "responder": "human",
                "answer": answer,
            },
        },
    )
    return True


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
