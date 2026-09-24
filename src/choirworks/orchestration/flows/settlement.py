# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_graph.py、utils/_graph_validation.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import structlog

from choirworks.core.context import build_assistance_decision_user
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.flows.engine import Edge, Flow, FlowOutcome
from choirworks.orchestration.functions import execute_function
from choirworks.orchestration.hitl.assist import spawn_assist
from choirworks.orchestration.hitl.intervention import emit_pending_questions
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    PENDING_NODE_STATUSES,
    InterventionStatus,
    NodeState,
    NodeStatus,
    QuestionType,
    add_intervention,
    input_required_nodes,
    pending_intervention_for,
)
from choirworks.orchestration.transitions import apply_transition
from choirworks.subagents.assistance import ASSISTANCE_SUBAGENT
from choirworks.subagents.base import run_subagent
from choirworks.tools.ask_user import AskUserArgs, ask_user_func
from choirworks.tools.outcome_decision import OutcomeDecision

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class SettlementPayload:
    node: NodeState
    decision: OutcomeDecision | None = None
    helper: NodeState | None = None


async def settle_input(ctx: OrchestrationContext) -> bool:
    """Resolve all ``input_required`` nodes (bulk wrapper for tests/recovery)."""
    progress = False
    for node in list(input_required_nodes(ctx.state)):
        if await settle_node_input(ctx, node):
            progress = True
    return progress


async def settle_node_input(ctx: OrchestrationContext, node: NodeState) -> bool:
    """Settle one ``input_required`` node: peer assistance or a human question."""
    outcome = await settlement_flow.run(ctx, SettlementPayload(node=node))
    return outcome is FlowOutcome.EXIT_DONE


async def _inspect_helpers(
    ctx: OrchestrationContext, payload: SettlementPayload
) -> Literal["already_pending", "helper_completed", "helper_active", "decide"]:
    state = ctx.state
    node = payload.node
    if pending_intervention_for(state, node.id) is not None:
        return "already_pending"
    helpers = [
        n
        for n in state.nodes.values()
        if n.derived and n.assist_requested_by == node.id and n.status == NodeStatus.COMPLETED
    ]
    if helpers:
        payload.helper = helpers[0]
        return "helper_completed"
    active_helpers = [
        n
        for n in state.nodes.values()
        if n.derived
        and n.assist_requested_by == node.id
        and n.status in ACTIVE_NODE_STATUSES | PENDING_NODE_STATUSES
    ]
    if active_helpers:
        return "helper_active"
    return "decide"


async def _noop(_ctx: OrchestrationContext, _payload: SettlementPayload) -> FlowOutcome:
    return FlowOutcome.END


async def _resolve_from_helper(
    ctx: OrchestrationContext, payload: SettlementPayload
) -> FlowOutcome:
    node = payload.node
    helper = payload.helper
    if helper is None:
        return FlowOutcome.END
    async with ctx.lock:
        intervention = pending_intervention_for(ctx.state, node.id)
        if intervention is None:
            intervention = add_intervention(ctx.state, node.id, node.question or "")
        intervention.status = InterventionStatus.RESOLVED
        intervention.answer = helper.output
        intervention.responder = helper.id
        node.answer_text = helper.output
        apply_transition(node, NodeStatus.READY)
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
    return FlowOutcome.EXIT_DONE


async def _decide_assistance(
    ctx: OrchestrationContext, payload: SettlementPayload
) -> Literal["act"]:
    payload.decision = await _decide_assistance_impl(ctx, payload.node)
    return "act"


async def _act(ctx: OrchestrationContext, payload: SettlementPayload) -> FlowOutcome:
    node = payload.node
    decision = payload.decision
    async with ctx.lock:
        if pending_intervention_for(ctx.state, node.id) is not None:
            return FlowOutcome.END
        if decision is not None and decision.target_agent:
            if await spawn_assist(ctx, node, decision):
                return FlowOutcome.EXIT_DONE
        await request_human(ctx, node, decision)
    return FlowOutcome.END


async def _decide_assistance_impl(
    ctx: OrchestrationContext, node: NodeState
) -> OutcomeDecision | None:
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
    _ = await execute_function(ctx, ask_user_func, args)
    await ctx.sessions.persist(ctx)
    await emit_pending_questions(ctx)


settlement_flow: Flow[SettlementPayload] = Flow(
    name="settlement_flow",
    start=_inspect_helpers,
    edges=(
        Edge(
            _inspect_helpers,
            _noop,
            frozenset({"already_pending", "helper_active"}),
        ),
        Edge(_inspect_helpers, _resolve_from_helper, frozenset({"helper_completed"})),
        Edge(_inspect_helpers, _decide_assistance, frozenset({"decide"})),
        Edge(_decide_assistance, _act, frozenset({"act"})),
    ),
)
