from .base_agent import BaseAgent


class LLMAgent(BaseAgent):
    """An agent that uses a large language model (LLM) to generate responses."""

    def __init__(self, llm):
        self.llm = llm

    async def _run_async_impl(self, ctx):
        # Implementation of the agent's asynchronous run method using the LLM.
        pass
