from collections.abc import AsyncGenerator
from typing import override

from a2a.types.a2a_pb2 import Message, Role, Part

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.models.llm_request import ChatMessage
from litellm.types.llms.openai import OpenAIChatCompletionSystemMessage
from new_choirworks.core.models.llm_request import LLMRequest
from new_choirworks.core.models.llm_response import LLMResponse


from .base_agent import BaseAgent


class LLMAgent(BaseAgent):
    """An agent that uses a large language model (LLM) to generate responses."""

    llm: BaseLLM
    instruction: str = ''

    @override
    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        # Implementation of the agent's asynchronous run method using the LLM.
        messages: list[ChatMessage] = []
        if self.instruction:
            messages.append(OpenAIChatCompletionSystemMessage(role="system", content=self.instruction))
        messages.extend(ctx.messages)

        req = LLMRequest(messages=messages)

        async for response in self.llm.generate_content_async(req, stream=True):
            msg = _to_a2a_message(response)
            yield msg


def _to_a2a_message(response: LLMResponse) -> Message:
    """Convert LLMResponse to A2A Message."""
    msg = Message()
    msg.role = Role.ROLE_AGENT
    if response.content:
        for part in response.content.parts:
            msg.parts.append(Part(text=part.text))

    return msg