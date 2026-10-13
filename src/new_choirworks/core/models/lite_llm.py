from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import cast, override

import litellm
from litellm.litellm_core_utils.streaming_handler import CustomStreamWrapper
from litellm.types.utils import ModelResponse

from .base_llm import BaseLLM
from .llm_request import LLMRequest
from .llm_response import LLMResponse, LLMResponseContent, LLMResponsePart

type CompletionResult = ModelResponse | CustomStreamWrapper
type CompletionFn = Callable[..., Awaitable[CompletionResult]]


class LiteLLM(BaseLLM):
    completion_fn: CompletionFn = cast(CompletionFn, litellm.acompletion)
    base_url: str | None = None

    @override
    async def generate_content_async(
        self, llm_request: LLMRequest, stream: bool = False
    ) -> AsyncGenerator[LLMResponse, None]:
        effective_model = llm_request.model or self.model
        response = await self.completion_fn(
            model=effective_model,
            messages=llm_request.messages,
            stream=stream,
            base_url=self.base_url,
        )

        if stream:
            if not isinstance(response, CustomStreamWrapper):
                raise ValueError("Expected a CustomStreamWrapper for streaming response.")
            async for chunk in response:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta
                parts: list[LLMResponsePart] = []
                reasoning: str | None = getattr(delta, "reasoning_content", None)
                if reasoning:
                    parts.append(LLMResponsePart(text=reasoning, thought=True))
                if delta.content:
                    parts.append(LLMResponsePart(text=delta.content, thought=False))
                if parts:
                    yield LLMResponse(content=LLMResponseContent(parts=parts))
