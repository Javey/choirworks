import abc
from collections.abc import AsyncGenerator

from pydantic import BaseModel, ConfigDict

from .llm_request import LlmRequest
from .llm_response import LLMResponse


class BaseLLM(BaseModel, abc.ABC):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    model: str

    @abc.abstractmethod
    async def generate_content_async(
        self, llm_request: LlmRequest, stream: bool = False
    ) -> AsyncGenerator[LLMResponse, None]:
        """Generate content from the LLM based on the provided request."""
        raise NotImplementedError("Subclasses must implement generate_content_async()")
