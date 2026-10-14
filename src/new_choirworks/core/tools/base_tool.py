import abc
from typing import ClassVar

from litellm.types.llms.openai import ChatCompletionToolParam
from pydantic import BaseModel, ConfigDict


class BaseTool(BaseModel, abc.ABC):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """Abstract base class for tools.

    Design reference: google-adk tools/base_tool.py (Apache-2.0, Copyright 2026 Google LLC).
    Uses _get_declaration() instead of a parameters field, so subclasses
    can generate the tool declaration dynamically.
    """

    model_config: ClassVar[ConfigDict] = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=True,
    )

    name: str
    description: str

    @abc.abstractmethod
    def _get_declaration(self) -> ChatCompletionToolParam:
        """Return the tool declaration in LLM tool format."""
        raise NotImplementedError

    @abc.abstractmethod
    async def run_async(self, args: dict[str, object]) -> object:
        """Execute the tool with the given arguments."""
        raise NotImplementedError
