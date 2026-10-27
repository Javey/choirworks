from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import cast, override

from a2a.helpers.proto_helpers import new_text_artifact_update_event
from a2a.server.events import EventQueue
from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent, NodeInfo, meta
from new_choirworks.core.workflow.base_node import BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.node_runner import NodeRunner
from new_choirworks.core.workflow.utils.replay_manager import ReplayManager


class _Queue(EventQueue):
    def __init__(self) -> None:
        self.events: list[object] = []

    @override
    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


class _StaticNode(BaseNode):
    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        _ = ctx, node_input
        yield new_text_artifact_update_event(
            task_id="t", context_id="c", name="art", text="done", last_chunk=True
        )


def _ctx(
    queue: _Queue,
    *,
    node_path: str = "wf@1",
    branch: str | None = None,
    isolation_scope: str | None = None,
) -> Context:
    return Context(
        invocation_context=InvocationContext(event_queue=queue),
        node=BaseNode(name="parent"),
        node_path=node_path,
        branch=branch,
        isolation_scope=isolation_scope,
    )


def _recorded(path: str, output: str) -> TaskArtifactUpdateEvent:
    event = new_text_artifact_update_event(
        task_id="t", context_id="c", name="art", text=output, last_chunk=True
    )
    meta(event).node_info = NodeInfo(path=path)
    meta(event).output = output
    return event


async def test_run_enriches_node_path_and_scope() -> None:
    queue = _Queue()
    runner = NodeRunner(node=_StaticNode(name="fake"), parent_ctx=_ctx(queue), run_id="2")

    ctx = await runner.run()

    assert ctx.node_path == "wf@1/fake@2"
    assert ctx.run_id == "2"
    event = cast(A2AEvent, queue.events[0])
    assert meta(event).author == "fake"
    assert meta(event).node_info.path == "wf@1/fake@2"


async def test_run_inherits_branch_and_isolation_scope() -> None:
    queue = _Queue()
    runner = NodeRunner(
        node=_StaticNode(name="fake"),
        parent_ctx=_ctx(queue, branch="a.b", isolation_scope="fc-1"),
    )

    _ = await runner.run()

    event = cast(A2AEvent, queue.events[0])
    assert meta(event).branch == "a.b"
    assert meta(event).isolation_scope == "fc-1"


async def test_run_replayed_emits_without_duplicating_log() -> None:
    queue = _Queue()
    event = _recorded("wf@1/a@1", "A")
    node = BaseNode(name="a")
    runner = NodeRunner(node=node, parent_ctx=_ctx(queue), run_id="1", parent_path="wf@1")

    ctx = await runner.run_replayed([event])

    assert queue.events == [event]
    assert ctx.output == "A"
    assert ctx.invocation_context.events == []


async def test_run_replayed_respects_sequence_barrier() -> None:
    queue = _Queue()
    a_event = _recorded("wf@1/a@1", "A")
    b_event = _recorded("wf@1/b@1", "B")
    manager = ReplayManager()
    _ = manager.prepare_parent_sequence_barrier([a_event, b_event], "wf@1")

    runner_a = NodeRunner(
        node=BaseNode(name="a"),
        parent_ctx=_ctx(queue),
        run_id="1",
        parent_path="wf@1",
        replay_manager=manager,
    )
    runner_b = NodeRunner(
        node=BaseNode(name="b"),
        parent_ctx=_ctx(queue),
        run_id="1",
        parent_path="wf@1",
        replay_manager=manager,
    )

    task_b = asyncio.create_task(runner_b.run_replayed([b_event]))
    await asyncio.sleep(0)
    assert not task_b.done()

    _ = await runner_a.run_replayed([a_event])
    _ = await asyncio.wait_for(task_b, timeout=0.1)

    assert queue.events == [a_event, b_event]
