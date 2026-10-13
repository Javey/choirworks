from collections.abc import AsyncGenerator, Awaitable, Callable

from litellm import acompletion
from litellm.litellm_core_utils.streaming_handler import CustomStreamWrapper
from litellm.types.utils import ModelResponse

from .base_llm import BaseLLM
from .llm_request import LlmRequest
from .llm_response import LLMResponse

type CompletionResult = ModelResponse | CustomStreamWrapper
type CompletionFn = Callable[..., Awaitable[CompletionResult]]


class LiteLLM(BaseLLM):
    completion_fn: CompletionFn = acompletion

    async def generate_content_async(
        self, llm_request: LlmRequest, stream: bool = False
    ) -> AsyncGenerator[LLMResponse, None]:
        effective_model = llm_request.model or self.model
        response = await self.completion_fn(
            model=effective_model,
            messages=llm_request.messages,
            stream=stream,
        )

        if stream:
            if not isinstance(response, CustomStreamWrapper):
                raise ValueError("Expected a CustomStreamWrapper for streaming response.")
            async for chunk in response:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta  # can not be None
                yield LLMResponse(content=delta)
