from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

from choirworks.orchestration.state import add_member
from choirworks.tools.base import AgentFunction, FunctionResult

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext


class JoinMembersArgs(BaseModel):
    """Arguments for ``join_members`` — add registered agents to the room."""

    names: list[str]
    reason: str


class JoinMembersData(BaseModel):
    """Result payload of ``join_members``."""

    joined: list[str]


async def join_members_args_model(ctx: OrchestrationContext) -> type[BaseModel]:
    return JoinMembersArgs


async def execute_join_members(ctx: OrchestrationContext, args: BaseModel) -> FunctionResult:
    """``join_members`` — add registered agents to the collaboration room.

    Pure state change: resolves the names against the registry, adds
    de-duplicated new members and returns the joined names.  The member
    ``state_delta`` is emitted by the orchestration layer that invokes this
    tool.  NOTE: if this function is ever exposed in a model's tool list, the
    calling path must emit that delta as well.
    """
    join_args = (
        args
        if isinstance(args, JoinMembersArgs)
        else (JoinMembersArgs.model_validate(args.model_dump()))
    )
    state = ctx.state
    records = await ctx.registry.list()
    known = {record.name: record for record in records}
    joined: list[str] = []
    for name in dict.fromkeys(join_args.names):
        record = known.get(name)
        if record is None:
            continue
        if not add_member(state, name, record.card_url, join_args.reason):
            continue
        joined.append(name)

    return FunctionResult(success=True, data=JoinMembersData(joined=joined))


join_members_func = AgentFunction(
    name="join_members",
    description="Add registered agents to the collaboration room as members.",
    args_model=join_members_args_model,
    execute=execute_join_members,
    is_long_running=False,
)
