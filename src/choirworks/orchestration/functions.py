from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from a2a.types.a2a_pb2 import TaskState

if TYPE_CHECKING:
    from pydantic import BaseModel

    from choirworks.orchestration.context import OrchestrationContext
    from choirworks.orchestration.state import MemberDelta
    from choirworks.tools.base import AgentFunction, FunctionResult

logger = structlog.get_logger(__name__)


async def join_members(
    ctx: OrchestrationContext,
    names: list[str],
    reason: str,
) -> None:
    """Add registered agents to the room via the ``join_members`` tool.

    The tool call runs (and its function-call artifact is emitted) through the
    standard path; a member ``state_delta`` follows for state sync.  No-op
    invocations (every requested name is already a member) record nothing.
    """
    from choirworks.orchestration.events import emit_state_delta
    from choirworks.tools.join_members import (
        JoinMembersArgs,
        JoinMembersData,
        join_members_func,
    )

    requested = [name for name in dict.fromkeys(names) if name not in ctx.state.members]
    if not requested:
        return
    logger.info(
        "join_members",
        requested=requested,
        reason=reason,
    )
    result = await execute_function(
        ctx, join_members_func, JoinMembersArgs(names=requested, reason=reason)
    )
    data = result.data
    joined = data.joined if isinstance(data, JoinMembersData) else []
    if not joined:
        return
    members: list[MemberDelta] = [
        {
            "agent_name": name,
            "agent_url": ctx.state.members[name].url,
            "reason": reason,
        }
        for name in joined
    ]
    await emit_state_delta(ctx, members=members)


async def execute_function(
    ctx: OrchestrationContext,
    func: AgentFunction,
    args: BaseModel,
    *,
    state_name: TaskState = TaskState.TASK_STATE_WORKING,
) -> FunctionResult:
    """Execute an AgentFunction and emit the function-call event.

    Calls execute with the orchestration context and emits the result artifact
    when ``func.emit_artifact`` is True.  Functions that deliver their outcome
    through their own state events (``emit_artifact=False``, e.g. ``ask_user``)
    emit nothing here — the caller owns the state event.
    """
    from choirworks.orchestration.events import emit_function_call

    logger.info(
        "execute_function",
        function=func.name,
    )
    result = await func.execute(ctx, args)
    logger.info(
        "execute_function done",
        function=func.name,
        success=result.success,
    )
    if func.emit_artifact:
        await emit_function_call(ctx, func, args, result, state_name=state_name)
    return result
