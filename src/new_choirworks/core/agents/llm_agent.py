import uuid
from collections.abc import AsyncGenerator
from typing import override

from litellm.types.llms.openai import OpenAIChatCompletionSystemMessage

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent, to_a2a_artifact
from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.models.llm_request import ChatMessage, LLMRequest
from new_choirworks.core.models.llm_response import (
    LLMResponse,
    LLMResponseContent,
    LLMResponsePart,
)

from .base_agent import BaseAgent


class LLMAgent(BaseAgent):
    """An agent that uses a large language model (LLM) to generate responses."""

    llm: BaseLLM
    instruction: str = ""

    @override
    async def _run_async_impl(
        self, ctx: InvocationContext
    ) -> AsyncGenerator[A2AEvent, None]:
        if not ctx.task_id:
            ctx.task_id = str(uuid.uuid4())
        if not ctx.context_id:
            ctx.context_id = str(uuid.uuid4())

        messages: list[ChatMessage] = []
        if self.instruction:
            messages.append(
                OpenAIChatCompletionSystemMessage(
                    role="system", content=self.instruction
                )
            )
        messages.extend(ctx.messages)

        req = LLMRequest(messages=messages)

        artifact_id = uuid.uuid4().hex
        thought_buf: list[str] = []
        text_buf: list[str] = []

        async for response in self.llm.generate_content_async(req, stream=True):
            if not response.content:
                continue
            yield to_a2a_artifact(
                ctx,
                response,
                artifact_id=artifact_id,
                append=bool(thought_buf) or bool(text_buf),
                last_chunk=False,
            )

            for part in response.content.parts:
                if part.text is None:
                    continue
                if part.thought:
                    thought_buf.append(part.text)
                else:
                    text_buf.append(part.text)

        final_response = self._close(thought_buf, text_buf)
        if final_response:
            yield to_a2a_artifact(
                ctx,
                final_response,
                artifact_id=artifact_id,
                append=False,
                last_chunk=True,
            )

    def _close(
        self,
        thought_buf: list[str],
        text_buf: list[str],
    ) -> LLMResponse | None:
        """Build the final aggregated response from accumulated text buffers."""
        final_parts: list[LLMResponsePart] = []
        if thought_buf:
            final_parts.append(
                LLMResponsePart(text="".join(thought_buf), thought=True)
            )
        if text_buf:
            final_parts.append(
                LLMResponsePart(text="".join(text_buf), thought=False)
            )
        if not final_parts:
            return None
        return LLMResponse(content=LLMResponseContent(parts=final_parts))

