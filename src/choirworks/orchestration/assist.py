from __future__ import annotations

import logging
import re

from choirworks.core.context import build_assist_input
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.flows import execute_function, join_members
from choirworks.orchestration.state import NodeState, assist_nodes_for
from choirworks.tools.call_subagent import CallSubagentArgs, call_subagent_func

logger = logging.getLogger(__name__)


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
            "arbitrate_mentions: node=%s mentions=%s",
            node.id, mentions,
        )
    for name in mentions:
        if name == node.agent_name or name not in known:
            continue
        if assist_nodes_for(state, name, node.id):
            continue
        if state.derived_count >= ctx.config.max_derived_nodes:
            logger.info(
                "arbitrate_mentions: node=%s max_derived reached, skipping @%s",
                node.id, name,
            )
            return
        state.derived_count += 1
        helper_id = f"{node.id}-a{state.derived_count}"
        helper = NodeState(
            id=helper_id,
            name="",
            agent_name=name,
            agent_url=known[name].card_url,
            deps=[],
            input_text=build_assist_input(node.agent_name, node.output),
            derived=True,
            assist_requested_by=node.id,
            source_message_id=node.id,
        )
        state.nodes[helper_id] = helper
        await join_members(ctx, [name], "agent_mention")
        await emit_state_delta(ctx, nodes={
            helper_id: {
                "status": "pending",
                "agent_name": helper.agent_name,
                "input_text": helper.input_text,
            },
        })
        await ctx.sessions.persist(ctx)
        ctx.runtime.runner_start_requested = True


async def spawn_assist(
    ctx: OrchestrationContext,
    node: NodeState,
    decision: object,
) -> bool:
    """Execute a CallSubagent function based on an assistance decision."""
    target = getattr(decision, "target_agent", "") or ""
    instruction = getattr(decision, "instruction", "")
    logger.info(
        "spawn_assist: node=%s target=%s instruction_len=%d",
        node.id, target, len(instruction),
    )
    args = CallSubagentArgs(
        requested_by=node.id,
        target_agent=target,
        instruction=instruction,
    )
    result = await execute_function(ctx, call_subagent_func, args)
    logger.info(
        "spawn_assist: node=%s success=%s",
        node.id, result.success,
    )
    return result.success
