from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from choirworks.a2a.context import OrchestrationContext
from choirworks.a2a.state import NodeState
from choirworks.core.context import build_assist_input
from choirworks.tools import CallSubagentFunction
from choirworks.tools.base import FunctionContext
from choirworks.tools.call_subagent import CallSubagentArgs

if TYPE_CHECKING:
    from choirworks.a2a.events import EventEmitter
    from choirworks.a2a.session import SessionManager

logger = logging.getLogger(__name__)


async def join_members(
    ctx: OrchestrationContext,
    names: list[str],
    reason: str,
) -> None:
    """Add agents to the room state (de-duplicated) and emit a member delta."""
    state = ctx.state
    records = await ctx.registry.list()
    known = {record.name: record for record in records}
    new_members: list[dict[str, Any]] = []
    for name in dict.fromkeys(names):
        record = known.get(name)
        if record is None:
            continue
        if not state.add_member(name, record.card_url, reason):
            continue
        new_members.append({
            "agent_name": name,
            "agent_url": record.card_url,
            "reason": reason,
        })
    if new_members:
        await ctx.emitter.emit_state_delta(ctx, members=new_members)


class AssistArbiter:
    """@mention arbitration and assist node spawning."""

    def __init__(
        self,
        emitter: EventEmitter,
        session_mgr: SessionManager,
        call_subagent_func: CallSubagentFunction,
        config: object,
    ):
        self._emitter = emitter
        self._session_mgr = session_mgr
        self._call_subagent_func = call_subagent_func
        self._config = config

    async def arbitrate_mentions(
        self,
        ctx: OrchestrationContext,
        node: NodeState,
    ) -> None:
        state = ctx.state
        if not node.output:
            return
        agents = await ctx.registry.list()
        known = {agent.name: agent for agent in agents}
        for name in dict.fromkeys(re.findall(r"@([A-Za-z0-9_-]+)", node.output)):
            if name == node.agent_name or name not in known:
                continue
            if state.assist_nodes_for(name, node.id):
                continue
            if state.derived_count >= self._config.max_derived_nodes:
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
            await self._emitter.emit_state_delta(ctx, nodes={
                helper_id: {
                    "status": "pending",
                    "agent_name": helper.agent_name,
                },
            })
            await self._session_mgr.persist(ctx)
            ctx.runtime.runner_start_requested = True

    async def spawn_assist(
        self,
        ctx: OrchestrationContext,
        node: NodeState,
        decision: object,
    ) -> bool:
        args = CallSubagentArgs(
            requested_by=node.id,
            target_agent=getattr(decision, "target_agent", "") or "",
            instruction=getattr(decision, "instruction", ""),
        )
        func_ctx = FunctionContext(executor=ctx.executor, runtime=ctx.runtime)  # type: ignore[arg-type]
        result = await self._call_subagent_func.execute(func_ctx, args)
        if not result.success:
            return False
        await self._emitter.emit_function_call(ctx, self._call_subagent_func, args, result)
        return True
