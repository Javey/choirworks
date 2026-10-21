from typing import override

from a2a.helpers.proto_helpers import (
    new_text_artifact_update_event,
    new_text_status_update_event,
)
from a2a.types.a2a_pb2 import Message, TaskState

from new_choirworks.core.agents.base_agent import BaseAgent
from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent, is_partial_event
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


def _artifact(*, append: bool, last_chunk: bool):
    return new_text_artifact_update_event(
        task_id="t1",
        context_id="c1",
        name="a",
        text="x",
        append=append,
        last_chunk=last_chunk,
    )


def test_is_partial_event():
    assert is_partial_event(_artifact(append=False, last_chunk=False)) is True
    assert is_partial_event(_artifact(append=True, last_chunk=False)) is True
    assert is_partial_event(_artifact(append=True, last_chunk=True)) is True
    assert is_partial_event(_artifact(append=False, last_chunk=True)) is False
    status = new_text_status_update_event(
        task_id="t1",
        context_id="c1",
        state=TaskState.TASK_STATE_WORKING,
        text="busy",
    )
    assert is_partial_event(status) is False
    assert is_partial_event(Message()) is False


async def test_run_async_does_not_track_events():
    partial = _artifact(append=False, last_chunk=False)
    final = _artifact(append=False, last_chunk=True)
    user = Message()
    agent = EchoAgent(events=[partial, final])
    ctx = InvocationContext(events=[user])

    yielded = [event async for event in agent.run_async(ctx)]

    assert yielded == [partial, final]
    assert ctx.events == [user]


async def test_run_as_node_does_not_track_parent_events():
    events: list[A2AEvent] = [Message(), Message()]
    agent = EchoAgent(events=events)
    ic = InvocationContext()
    ctx = Context(invocation_context=ic, node=agent)

    yielded = [event async for event in agent.run(ctx)]

    assert yielded == events
    assert ic.events == []
