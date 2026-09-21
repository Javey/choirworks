from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

if TYPE_CHECKING:
    from choirworks.a2a.executor import ChoirWorksAgentExecutor, SessionRuntime
    from choirworks.a2a.registry import AgentRegistry


class FunctionResult(BaseModel):
    """The outcome of executing an :class:`AgentFunction`.

    For long-running functions ``data`` carries the immediate ack payload;
    the real result arrives later via state deltas.
    """

    success: bool
    data: dict[str, Any] | None = None
    error: str | None = None


@dataclass
class ToolCallResult:
    """The LLM's parsed tool call — function + validated args.

    Yielded by :meth:`LiteLLMClient.stream` when the model invokes a tool.
    The caller is responsible for executing the function.
    """

    function: AgentFunction
    args: BaseModel


@dataclass
class FunctionContext:
    """Dependencies handed to every :class:`AgentFunction` at call time.

    The executor is passed directly so functions can reach shared helpers
    (``_join_members``, ``_start_runner``, ``_persist`` …) without the
    executor needing to enumerate every capability up front.
    """

    executor: ChoirWorksAgentExecutor
    runtime: SessionRuntime

    @property
    def state(self) -> Any:
        return self.runtime.state

    @property
    def registry(self) -> AgentRegistry:
        return self.executor._registry


class AgentFunction(ABC):
    """A callable capability that the orchestrator model can invoke.

    Each subclass bundles three things:

    * ``args_model`` – the Pydantic schema used both as the LLM tool
      declaration (forced via ``stream_structured``) and as the wire-format
      payload sent to the frontend.
    * ``execute`` – the side effects to run when the model calls this
      function.
    * ``is_long_running`` – when *True* the function returns an ack
      immediately; the real result propagates later as state deltas.
      The model is **not** kept in the loop waiting for a response.
    """

    name: str
    """Wire-format identifier (``"create_plan"``, ``"ask_user"`` …)."""

    description: str
    """Human / LLM description of what this function does."""

    is_long_running: bool = False

    @abstractmethod
    async def args_model(self, ctx: FunctionContext) -> type[BaseModel]:
        """Return the Pydantic schema for this function's arguments.

        May be dynamic — e.g. ``create_plan`` constrains ``agent_name`` to a
        ``Literal`` of currently-registered agents — hence ``async``.
        """

    @abstractmethod
    async def execute(
        self, ctx: FunctionContext, args: BaseModel
    ) -> FunctionResult:
        """Run the function's side effects and return an ack / result."""

    async def declaration(self, ctx: FunctionContext) -> dict[str, Any]:
        """Auto-build an LLM function-tool declaration from ``args_model``."""
        schema = await self.args_model(ctx)
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": schema.model_json_schema(),
            },
        }
