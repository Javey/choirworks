from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import structlog
from pydantic import BaseModel, Field

from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.helpers import join_members
from choirworks.orchestration.hitl.intervention import emit_pending_questions
from choirworks.orchestration.state import (
    InterventionDelta,
    NodeState,
    NodeStatus,
    OrchestrationState,
    add_cancel_request,
)
from choirworks.orchestration.transitions import apply_transition

logger = structlog.get_logger(__name__)

INVALIDATABLE_STATUSES = {
    NodeStatus.PENDING,
    NodeStatus.READY,
    NodeStatus.RECOVER,
    NodeStatus.FAILED,
}


class PatchNode(BaseModel):
    agent_name: str
    name: str = ""
    instruction: str
    deps: list[str] = Field(default_factory=list)


class PlanPatch(BaseModel):
    add: list[PatchNode] = Field(default_factory=list)
    invalidate: list[str] = Field(default_factory=list)
    reason: str = ""


@dataclass
class PatchResult:
    added: list[str] = field(default_factory=list)
    invalidated: list[str] = field(default_factory=list)
    skipped_in_flight: list[str] = field(default_factory=list)
    rejected: list[str] = field(default_factory=list)


def apply_patch(
    state: OrchestrationState,
    patch: PlanPatch,
    agent_urls: Mapping[str, str],
) -> PatchResult:
    """Apply an incremental plan patch, keeping finished work intact."""
    result = PatchResult()

    for draft in patch.add:
        if draft.agent_name not in agent_urls:
            result.rejected.append(f"unknown agent: {draft.agent_name}")
            continue
        unknown = [dep for dep in draft.deps if dep not in state.nodes]
        if unknown:
            result.rejected.append(f"unknown deps for {draft.agent_name}: {', '.join(unknown)}")
            continue
        state.patch_count += 1
        node_id = f"x{state.patch_count}"
        while node_id in state.nodes:
            state.patch_count += 1
            node_id = f"x{state.patch_count}"
        state.nodes[node_id] = NodeState(
            id=node_id,
            name=draft.name or draft.agent_name,
            agent_name=draft.agent_name,
            agent_url=agent_urls[draft.agent_name],
            deps=list(draft.deps),
            input_text=draft.instruction,
            derived=True,
        )
        result.added.append(node_id)

    invalidated: list[str] = []
    invalidated_set: set[str] = set()
    for node_id in patch.invalidate:
        node = state.nodes.get(node_id)
        if node is None:
            result.rejected.append(f"unknown node: {node_id}")
            continue
        if node.status in INVALIDATABLE_STATUSES:
            apply_transition(node, NodeStatus.INVALIDATED)
            invalidated.append(node_id)
            invalidated_set.add(node_id)
        elif node.status != NodeStatus.INVALIDATED:
            result.skipped_in_flight.append(node_id)

    while True:
        cascaded = [
            node
            for node in state.nodes.values()
            if node.status in INVALIDATABLE_STATUSES
            and any(dep in invalidated_set for dep in node.deps)
        ]
        if not cascaded:
            break
        for node in cascaded:
            apply_transition(node, NodeStatus.INVALIDATED)
            invalidated.append(node.id)
            invalidated_set.add(node.id)

    result.invalidated = invalidated
    return result


async def apply_patch_locked(ctx: OrchestrationContext, patch: PlanPatch) -> PatchResult:
    agents = await ctx.registry.list()
    agent_urls = {agent.name: agent.card_url for agent in agents}
    state = ctx.state
    result = apply_patch(state, patch, agent_urls)
    for rejected in result.rejected:
        logger.warning("Patch rejected", context=ctx.context_id, rejected=rejected)
    new_interventions: dict[str, InterventionDelta] = {}
    for node_id in result.skipped_in_flight:
        node = state.nodes.get(node_id)
        if node is None:
            continue
        question = (
            f"计划修订建议作废进行中的任务 @{node.agent_name}"
            f"（{patch.reason or '无说明'}）。是否打断？"
            "回复「确认」打断，回复其他内容则保留。"
        )
        intervention = add_cancel_request(state, node_id, question)
        if intervention is None:
            continue
        new_interventions[intervention.id] = {
            "status": "pending",
            "node_id": node_id,
            "kind": "confirm_cancel",
            "question": intervention.question,
        }
    if new_interventions:
        await emit_state_delta(ctx, interventions=new_interventions)
    added_agents = [draft.agent_name for draft in patch.add if draft.agent_name in agent_urls]
    await join_members(ctx, added_agents, "plan_revision")
    if result.invalidated:
        await emit_state_delta(
            ctx, nodes={node_id: {"status": "invalidated"} for node_id in result.invalidated}
        )
    await ctx.sessions.persist(ctx)
    await emit_pending_questions(ctx)
    return result
