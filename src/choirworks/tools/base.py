from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from choirworks.tools.capabilities import ToolEffects

if TYPE_CHECKING:
    from choirworks.orchestration.registry import AgentRegistry
    from choirworks.orchestration.session import SessionRuntime
    from choirworks.orchestration.state import OrchestrationState


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


@dataclass(frozen=True, slots=True)
class FunctionContext:
    """Dependencies handed to every :class:`AgentFunction` at call time.

    Bundles the session runtime, the agent registry, and the side effects the
    orchestrator permits a tool to perform (:class:`ToolEffects`).  Tools
    therefore never reach back into the executor.
    """

    runtime: SessionRuntime
    registry: AgentRegistry
    effects: ToolEffects

    @property
    def state(self) -> OrchestrationState:
        return self.runtime.state


@dataclass(frozen=True, slots=True)
class AgentFunction:
    """A callable capability that the orchestrator model can invoke.

    A data-only record composing three things:

    * ``args_model`` – returns the Pydantic schema used both as the LLM tool
      declaration (forced via ``stream_structured``) and as the wire-format
      payload sent to the frontend.  May be dynamic — e.g. ``create_plan``
      constrains ``agent_name`` to a ``Literal`` of currently-registered
      agents — hence ``async`` and callable rather than a class attribute.
    * ``execute`` – the side effects to run when the model calls this
      function.
    * ``is_long_running`` – when *True* the function returns an ack
      immediately; the real result propagates later as state deltas.
      The model is **not** kept in the loop waiting for a response.

    ``name`` is the wire-format identifier (``"create_plan"``, ``"ask_user"``
    …); ``description`` is the human / LLM facing description.
    """

    name: str
    description: str
    args_model: Callable[[FunctionContext], Awaitable[type[BaseModel]]]
    execute: Callable[[FunctionContext, BaseModel], Awaitable[FunctionResult]]
    is_long_running: bool = False
