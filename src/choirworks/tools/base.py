from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import BaseModel, SerializeAsAny

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext


class FunctionResult(BaseModel):
    """The outcome of executing an :class:`AgentFunction`.

    For long-running functions ``data`` carries the immediate ack payload;
    the real result arrives later via state deltas.
    """

    success: bool
    data: SerializeAsAny[BaseModel] | None = None
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
    * ``emit_artifact`` – when *False* the function call is not surfaced as a
      ``function_call`` artifact; callers emit their own state events
      (e.g. ``ask_user`` delivers its question via ``status.message``).

    ``name`` is the wire-format identifier (``"create_plan"``, ``"ask_user"``
    …); ``description`` is the human / LLM facing description.
    """

    name: str
    description: str
    args_model: Callable[[OrchestrationContext], Awaitable[type[BaseModel]]]
    execute: Callable[[OrchestrationContext, BaseModel], Awaitable[FunctionResult]]
    is_long_running: bool = False
    emit_artifact: bool = True
