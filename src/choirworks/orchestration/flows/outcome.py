# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_graph.py、utils/_graph_validation.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

import structlog

from choirworks.core.agents.base import run_agent
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import record_decision
from choirworks.orchestration.flows.engine import Edge, Flow, FlowOutcome, Route
from choirworks.orchestration.hitl.assist import arbitrate_mentions
from choirworks.orchestration.planning.derived import spawn_followup_node
from choirworks.orchestration.planning.repair import revise_plan
from choirworks.orchestration.prompts import build_outcome_user
from choirworks.orchestration.state import NodeState, NodeStatus, take_queued
from choirworks.orchestration.transitions import transition
from choirworks.subagents.outcome.agent import outcome_agent
from choirworks.subagents.outcome.model import OutcomeResult

logger = structlog.get_logger(__name__)

MarkerIntent = Literal["deliver", "need_info", "revise"]

_MARKER_RE = re.compile(
    r"^\[cw:(deliver|need_info|assist|revise)\][ \t]*(.*)$",
    re.MULTILINE,
)
_INTENT_MAP: dict[str, MarkerIntent] = {
    "deliver": "deliver",
    "need_info": "need_info",
    "assist": "need_info",
    "revise": "revise",
}


@dataclass(frozen=True)
class Marker:
    intent: MarkerIntent
    text: str = ""


def parse_marker(output: str | None) -> Marker | None:
    """Parse a receipt marker from the first non-empty line of an output.

    Only the first non-empty line is considered so that an agent echoing the
    dispatch text (which contains marker examples) cannot trigger a marker.
    """
    if not output:
        return None
    for line in output.splitlines():
        if not line.strip():
            continue
        match = _MARKER_RE.match(line)
        if match is None:
            return None
        return Marker(
            intent=_INTENT_MAP[match.group(1)],
            text=match.group(2).strip(),
        )
    return None


@dataclass(slots=True)
class OutcomePayload:
    node: NodeState
    decision: OutcomeResult | None = None


async def _interpret(
    ctx: OrchestrationContext, payload: OutcomePayload
) -> Literal["deliver", "need_info", "revise"]:
    decision = await _interpret_outcome(ctx, payload.node)
    payload.decision = decision
    logger.info(
        "handle_completed",
        context_id=ctx.context_id,
        node_id=payload.node.id,
        intent=decision.intent,
    )
    if decision.intent == "need_info":
        return "need_info"
    if decision.intent == "revise":
        return "revise"
    return "deliver"


async def _mark_input_required(ctx: OrchestrationContext, payload: OutcomePayload) -> FlowOutcome:
    node = payload.node
    decision = payload.decision
    node.question = (decision.question if decision else None) or node.output
    node.a2a_task_id = None
    await transition(
        ctx,
        node,
        NodeStatus.INPUT_REQUIRED,
        delta={"question": node.question or "", "agent_name": node.agent_name},
    )
    return FlowOutcome.END


async def _revise(ctx: OrchestrationContext, payload: OutcomePayload) -> Route:
    decision = payload.decision
    if decision is not None and decision.intent == "revise" and decision.patch is not None:
        if ctx.state.revision_count < ctx.config.max_revisions:
            async with ctx.lock:
                _ = await revise_plan(ctx, decision.patch)
        else:
            logger.warning("Revision limit reached, skipping", context_id=ctx.context_id)
    return ""


async def _mark_delivered(ctx: OrchestrationContext, payload: OutcomePayload) -> Route:
    node = payload.node
    await transition(
        ctx,
        node,
        NodeStatus.COMPLETED,
        delta={"agent_name": node.agent_name, "output": (node.output or "")[:200]},
    )
    return ""


async def _arbitrate(ctx: OrchestrationContext, payload: OutcomePayload) -> Route:
    await arbitrate_mentions(ctx, payload.node)
    return ""


async def _deliver_queued(ctx: OrchestrationContext, payload: OutcomePayload) -> FlowOutcome:
    node = payload.node
    messages = take_queued(ctx.state, node.id)
    if messages:
        text = "\n\n".join(message.text for message in messages)
        await spawn_followup_node(ctx, text, node, deps=[node.id])
    return FlowOutcome.END


async def _interpret_outcome(ctx: OrchestrationContext, node: NodeState) -> OutcomeResult:
    marker = parse_marker(node.output)
    if marker is not None:
        await record_decision(ctx, f"[outcome:{node.id}] {marker.intent}")
        return OutcomeResult(intent=marker.intent, question=marker.text)
    if not node.output:
        await record_decision(ctx, f"[outcome:{node.id}] deliver")
        return OutcomeResult(intent="deliver")
    agents = await ctx.registry.list()
    candidates = [agent for agent in agents if agent.name != node.agent_name]
    user = build_outcome_user(node.agent_name, node.input_text, node.output, candidates)
    try:
        result = await run_agent(
            outcome_agent,
            ctx,
            user,
            exclude_agent=node.agent_name,
        )
    except Exception:
        logger.exception(
            "outcome interpretation failed", context_id=ctx.context_id, node_id=node.id
        )
        await record_decision(ctx, f"[outcome:{node.id}] deliver")
        return OutcomeResult(intent="deliver")
    decision = result if result is not None else OutcomeResult(intent="deliver")
    await record_decision(ctx, f"[outcome:{node.id}] {decision.intent}")
    return decision


outcome_flow: Flow[OutcomePayload] = Flow(
    name="outcome_flow",
    start=_interpret,
    edges=(
        Edge(_interpret, _mark_delivered, frozenset({"deliver"})),
        Edge(_interpret, _mark_input_required, frozenset({"need_info"})),
        Edge(_interpret, _revise, frozenset({"revise"})),
        Edge(_revise, _mark_delivered),
        Edge(_mark_delivered, _arbitrate),
        Edge(_arbitrate, _deliver_queued),
    ),
)
