from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pydantic import BaseModel, SerializeAsAny

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext


class FunctionResult(BaseModel):
    """The outcome of executing a :class:`FunctionTool`."""

    success: bool
    data: SerializeAsAny[BaseModel] | None = None
    error: str | None = None


@dataclass
class ToolCallResult:
    """The LLM's parsed tool call — function + validated args.

    Yielded by :meth:`LiteLLMClient.stream` when the model invokes a tool.
    The caller is responsible for executing the function.
    """

    function: FunctionTool
    args: BaseModel


class FunctionTool:
    """A callable capability that the model can invoke (ADK FunctionTool shape).

    Subclasses set class attributes ``name`` / ``description`` /
    ``emit_artifact`` and override :meth:`_get_declaration` / :meth:`run_async`.
    Construction-time keyword arguments override class attributes; omitting a
    keyword keeps the class-level default.
    """

    name: str
    description: str
    emit_artifact: bool = True

    def __init__(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        emit_artifact: bool | None = None,
    ) -> None:
        if name is not None:
            self.name = name
        if not hasattr(self, "name"):
            raise TypeError(f"{type(self).__name__} requires a 'name'")
        if description is not None:
            self.description = description
        if emit_artifact is not None:
            self.emit_artifact = emit_artifact

    async def _get_declaration(self, ctx: OrchestrationContext) -> type[BaseModel]:
        """Return the Pydantic schema used as the LLM tool declaration."""
        raise NotImplementedError

    async def run_async(self, ctx: OrchestrationContext, args: BaseModel) -> FunctionResult:
        """Execute the tool."""
        raise NotImplementedError


class StructuredOutputTool(FunctionTool):
    """Force the LLM to return structured data via a no-side-effect tool call.

    The model is forced to call this tool (via ``tool_choice``), and the
    validated args schema is the structured output.  The caller reads the
    result from :class:`ToolCallResult.args` directly; ``run_async`` is a
    no-op returning ``FunctionResult(success=True)``.
    """

    def __init__(self, *, name: str, description: str, schema: type[BaseModel]) -> None:
        super().__init__(name=name, description=description)
        self._schema = schema

    async def _get_declaration(self, ctx: OrchestrationContext) -> type[BaseModel]:
        return self._schema

    async def run_async(self, ctx: OrchestrationContext, args: BaseModel) -> FunctionResult:
        return FunctionResult(success=True)
