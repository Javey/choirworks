from __future__ import annotations

import re

import structlog

from choirworks.core.context import build_assist_input
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.derived import DerivedKind, spawn_derived_node
from choirworks.orchestration.functions import execute_function
from choirworks.orchestration.state import NodeState, assist_nodes_for
from choirworks.tools.call_subagent import CallSubagentArgs, call_subagent_func

logger = structlog.get_logger(__name__)


async def arbitrate_mentions(
    ctx: OrchestrationContext,
    node: NodeState,
) -> None:
    """Scan node.output for @mentions and spawn derived assist nodes."""
    state = ctx.state
    if not node.output:
        return
    agents = await ctx.registry.list()
    known = {agent.name: agent for agent in agents}
    mentions = list(dict.fromkeys(re.findall(r"@([A-Za-z0-9_-]+)", node.output)))
    if mentions:
        logger.info(
            "arbitrate_mentions",
            node=node.id,
            mentions=mentions,
        )
    for name in mentions:
        if name == node.agent_name or name not in known:
            continue
        if assist_nodes_for(state, name, node.id):
            continue
        helper = await spawn_derived_node(
            ctx,
            DerivedKind.ASSIST,
            parent_id=node.id,
            agent_name=name,
            agent_url=known[name].card_url,
            input_text=build_assist_input(node.agent_name, node.output),
            assist_requested_by=node.id,
            source_message_id=node.id,
        )
        if helper is None:
            return


async def spawn_assist(
    ctx: OrchestrationContext,
    node: NodeState,
    decision: object,
) -> bool:
    """Execute a CallSubagent function based on an assistance decision."""
    target = getattr(decision, "target_agent", "") or ""
    instruction = getattr(decision, "instruction", "")
    logger.info(
        "spawn_assist",
        node=node.id,
        target=target,
        instruction_len=len(instruction),
    )
    args = CallSubagentArgs(
        requested_by=node.id,
        target_agent=target,
        instruction=instruction,
    )
    result = await execute_function(ctx, call_subagent_func, args)
    logger.info(
        "spawn_assist",
        node=node.id,
        success=result.success,
    )
    return result.success
