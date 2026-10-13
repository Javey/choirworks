import abc
from collections.abc import AsyncGenerator
from contextlib import aclosing
from typing import ClassVar

from pydantic import BaseModel, ConfigDict

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent


class BaseAgent(BaseModel, abc.ABC):  # pyright: ignore[reportUnsafeMultipleInheritance]
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
    )

    name: str
    description: str = ""

    async def run_async(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        """Run the agent asynchronously, yielding events as they occur."""
        async with aclosing(self._run_async_impl(ctx)) as agen:
            async for event in agen:
                yield event

    @abc.abstractmethod
    def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        """Run the agent asynchronously, yielding events as they occur."""
        raise NotImplementedError("subclasses must implement _run_async_impl()")
