from typing import override

from a2a.types.a2a_pb2 import Message

from new_choirworks.core.agents.base_agent import BaseAgent
from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.context import Context


class EchoAgent(BaseAgent):
    name: str = "echo"
    description: str = "An agent that echoes messages."
    events: list[A2AEvent]

    @override
    async def _run_async_impl(self, ctx: InvocationContext):
        for event in self.events:
            yield event


async def test_run_async_yields_events_in_order():
    events: list[A2AEvent] = [Message(), Message()]
    agent = EchoAgent(events=events)
    ctx = InvocationContext()

    yielded_events: list[A2AEvent] = []
    async for event in agent.run_async(ctx):
        yielded_events.append(event)

    assert yielded_events == events


async def test_run_as_node_bridges_to_run_async():
    events: list[A2AEvent] = [Message(), Message()]
    agent = EchoAgent(events=events)
    ctx = Context(invocation_context=InvocationContext(), node=agent)

    yielded_events: list[A2AEvent] = []
    async for event in agent.run(ctx):
        yielded_events.append(event)

    assert yielded_events == events
