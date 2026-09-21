from __future__ import annotations

from typing import TYPE_CHECKING

from a2a.types.a2a_pb2 import TaskState

if TYPE_CHECKING:
    from pydantic import BaseModel

    from choirworks.orchestration.context import OrchestrationContext
    from choirworks.orchestration.state import MemberDelta
    from choirworks.tools.base import AgentFunction, FunctionResult


async def join_members(
    ctx: OrchestrationContext,
    names: list[str],
    reason: str,
) -> None:
    """Add agents to the room state (de-duplicated) and emit a member delta."""
    from choirworks.orchestration.events import emit_state_delta
    from choirworks.orchestration.state import add_member

    state = ctx.state
    records = await ctx.registry.list()
    known = {record.name: record for record in records}
    new_members: list[MemberDelta] = []
    for name in dict.fromkeys(names):
        record = known.get(name)
        if record is None:
            continue
        if not add_member(state, name, record.card_url, reason):
            continue
        new_members.append({
            "agent_name": name,
            "agent_url": record.card_url,
            "reason": reason,
        })
    if new_members:
        await emit_state_delta(ctx, members=new_members)


async def execute_function(
    ctx: OrchestrationContext,
    func: AgentFunction,
    args: BaseModel,
    *,
    state_name: TaskState = TaskState.TASK_STATE_WORKING,
) -> FunctionResult:
    """Execute an AgentFunction and emit the function-call event.

    Builds the FunctionContext, calls execute, emits the result artifact,
    and returns the FunctionResult for the caller to inspect.
    """
    from choirworks.orchestration.events import emit_function_call
    from choirworks.tools.base import FunctionContext

    func_ctx = FunctionContext(
        runtime=ctx.runtime,
        registry=ctx.registry,
        effects=ctx.effects,
    )
    result = await func.execute(func_ctx, args)
    await emit_function_call(ctx, func, args, result, state_name=state_name)
    return result
