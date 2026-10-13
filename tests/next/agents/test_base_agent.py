from a2a.types.a2a_pb2 import Message

from new_choirworks.core.agents.base_agent import BaseAgent
from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent


class EchoAgent(BaseAgent):
    name: str = "echo"
    description: str = "An agent that echoes messages."

    def __init__(self, events: list[A2AEvent]):
        self._events = events

    async def _run_async_impl(self, ctx: InvocationContext):
        for event in self._events:
            yield event


async def test_run_async_yields_events_in_order():
    events = [Message(), Message()]
    agent = EchoAgent(events)
    ctx = InvocationContext()

    yielded_events = []
    async for event in agent.run_async(ctx):
        yielded_events.append(event)

    assert yielded_events == events
