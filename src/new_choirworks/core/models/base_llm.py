import abc
from collections.abc import AsyncGenerator
from typing import ClassVar

from pydantic import BaseModel, ConfigDict

from .llm_request import LLMRequest
from .llm_response import LLMResponse


class BaseLLM(BaseModel, abc.ABC): # pyright: ignore[reportUnsafeMultipleInheritance]
    model_config: ClassVar[ConfigDict] = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    model: str

    @abc.abstractmethod
    def generate_content_async(
        self, llm_request: LLMRequest, stream: bool = False
    ) -> AsyncGenerator[LLMResponse, None]:
        """Generate content from the LLM based on the provided request."""
        raise NotImplementedError("Subclasses must implement generate_content_async()")
