from collections.abc import AsyncGenerator
from typing import override

from a2a.types.a2a_pb2 import Message

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.models.base_llm import BaseLLM

from .base_agent import BaseAgent


class LLMAgent(BaseAgent):
    """An agent that uses a large language model (LLM) to generate responses."""
    llm: BaseLLM

    @override
    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        # Implementation of the agent's asynchronous run method using the LLM.
        yield Message()
