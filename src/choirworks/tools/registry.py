from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from choirworks.tools.base import AgentFunction, FunctionContext


class FunctionRegistry:
    """Maps function names to :class:`AgentFunction` instances.

    The executor registers functions at construction time and looks them up
    by wire-format name when emitting events.
    """

    def __init__(self) -> None:
        self._functions: dict[str, AgentFunction] = {}

    def register(self, func: AgentFunction) -> AgentFunction:
        if func.name in self._functions:
            raise ValueError(f"duplicate function: {func.name}")
        self._functions[func.name] = func
        return func

    def get(self, name: str) -> AgentFunction | None:
        return self._functions.get(name)

    def names(self) -> list[str]:
        return list(self._functions)

    async def declarations(self, ctx: FunctionContext) -> list[dict[str, object]]:
        return [await func.declaration(ctx) for func in self._functions.values()]
