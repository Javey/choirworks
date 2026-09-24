from __future__ import annotations

from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.derived import DerivedKind, spawn_derived_node
from choirworks.orchestration.state import NodeState


async def spawn_followup_node(
    ctx: OrchestrationContext,
    text: str,
    anchor: NodeState,
    *,
    deps: list[str] | None = None,
) -> None:
    """Create a derived follow-up node handled by the anchor's agent."""
    _ = await spawn_derived_node(
        ctx,
        DerivedKind.FOLLOWUP,
        parent_id=anchor.id,
        agent_name=anchor.agent_name,
        agent_url=anchor.agent_url,
        input_text=text,
        deps=deps if deps is not None else [anchor.id],
    )
