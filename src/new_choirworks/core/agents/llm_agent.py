import uuid
from collections.abc import AsyncGenerator
from typing import override

from litellm.types.llms.openai import OpenAIChatCompletionSystemMessage

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent, to_a2a_artifact
from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.models.llm_request import ChatMessage, LLMRequest

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
        async for response in self.llm.generate_content_async(req, stream=True):
            # print(response)
            yield to_a2a_artifact(ctx, response, artifact_id=artifact_id)

