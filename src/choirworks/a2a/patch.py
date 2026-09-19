from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from pydantic import BaseModel, Field

from choirworks.a2a.state import NodeState, OrchestrationState

INVALIDATABLE_STATUSES = {"pending", "ready", "resume", "failed"}


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
            result.rejected.append(
                f"unknown deps for {draft.agent_name}: {', '.join(unknown)}"
            )
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
            node.status = "invalidated"
            invalidated.append(node_id)
            invalidated_set.add(node_id)
        elif node.status not in {"invalidated"}:
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
            node.status = "invalidated"
            invalidated.append(node.id)
            invalidated_set.add(node.id)

    result.invalidated = invalidated
    return result
