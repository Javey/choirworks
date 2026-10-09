from __future__ import annotations

from typing import TYPE_CHECKING, override

from pydantic import BaseModel

from choirworks.core.tool import FunctionResult, FunctionTool
from choirworks.orchestration.state import add_member

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext


class JoinMembersArgs(BaseModel):
    """Arguments for ``join_members`` — add registered agents to the room."""

    names: list[str]
    reason: str


class JoinMembersData(BaseModel):
    """Result payload of ``join_members``."""

    joined: list[str]


class JoinMembersTool(FunctionTool):
    name = "join_members"
    description = "Add registered agents to the collaboration room as members."

    @override
    async def _get_declaration(self, ctx: OrchestrationContext) -> type[BaseModel]:
        return JoinMembersArgs

    @override
    async def run_async(self, ctx: OrchestrationContext, args: BaseModel) -> FunctionResult:
        join_args = (
            args
            if isinstance(args, JoinMembersArgs)
            else JoinMembersArgs.model_validate(args.model_dump())
        )
        state = ctx.state
        records = await ctx.registry.list()
        known = {record.name: record for record in records}
        joined: list[str] = []
        for name in dict.fromkeys(join_args.names):
            record = known.get(name)
            if record is None:
                continue
            if not add_member(state, name, join_args.reason):
                continue
            joined.append(name)
        return FunctionResult(success=True, data=JoinMembersData(joined=joined))


join_members_func = JoinMembersTool()
