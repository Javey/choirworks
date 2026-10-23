from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import ClassVar, final, override

import pytest
from a2a.helpers.proto_helpers import new_text_status_update_event
from a2a.server.events import EventQueue
from a2a.types.a2a_pb2 import TaskState, TaskStatusUpdateEvent

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import (
    NON_FINAL_TASK_STATES,
    A2AEvent,
    add_author,
    extract_text,
    is_partial_event,
    meta,
)
from new_choirworks.core.workflow.base_node import BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.orchestrator import executor_node
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.executor_node import RemoteNodeError, run_remote_node
from new_choirworks.orchestrator.plan import PlanNode

AGENT = AgentInfo(name="reviewer", url="http://reviewer")


@final
class _FakeRemoteA2aAgent:
    """模拟 RemoteA2aAgent 的 run/cleanup 契约（含 author 标记与 output 提升语义）。"""

    stream: ClassVar[list[A2AEvent]] = []
    instances: ClassVar[list[_FakeRemoteA2aAgent]] = []

    def __init__(self, name: str, agent_url: str) -> None:
        self.name: str = name
        self.agent_url: str = agent_url
        self.seen_ctx: list[InvocationContext] = []
        self.cleaned: bool = False
        _FakeRemoteA2aAgent.instances.append(self)

    async def run(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        _ = node_input
        self.seen_ctx.append(ctx.invocation_context)
        for event in _FakeRemoteA2aAgent.stream:
            event = add_author(event, self.name)
            if (
                not is_partial_event(event)
                and ctx.output is None
                and extract_text(event)
                and not _is_non_final_status(event)
            ):
                ctx.output = extract_text(event)
            yield event

    async def cleanup(self) -> None:
        self.cleaned = True


def _is_non_final_status(event: A2AEvent) -> bool:
    return isinstance(event, TaskStatusUpdateEvent) and event.status.state in NON_FINAL_TASK_STATES


@pytest.fixture(autouse=True)
def _patch_remote_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeRemoteA2aAgent.stream = []
    _FakeRemoteA2aAgent.instances = []
    monkeypatch.setattr(executor_node, "RemoteA2aAgent", _FakeRemoteA2aAgent)


def _event(state: TaskState, text: str) -> A2AEvent:
    return new_text_status_update_event(task_id="t1", context_id="c1", state=state, text=text)


def _ic(event_queue: EventQueue | None = None) -> InvocationContext:
    return InvocationContext(
        events=[],
        event_queue=event_queue,
        task_id="parent-task",
        context_id="parent-ctx",
    )


def _node() -> PlanNode:
    return PlanNode(id="n1", name="Review draft", agent_name="reviewer", instruction="check")


async def _run(event_queue: EventQueue | None = None) -> tuple[str, InvocationContext]:
    ic = _ic(event_queue)
    ctx = Context(invocation_context=ic, node=BaseNode(name="test"))
    output = await run_remote_node(ctx, _node(), AGENT, "please check")
    return output, ic


async def test_success_returns_output_and_cleans_up():
    _FakeRemoteA2aAgent.stream = [_event(TaskState.TASK_STATE_COMPLETED, "LGTM")]
    output, _icx = await _run()

    assert output == "LGTM"
    agent = _FakeRemoteA2aAgent.instances[0]
    assert agent.cleaned is True
    # 身份用 agent_name 而非展示名
    assert agent.name == "reviewer"
    # 子 invocation 沿用父 task/context id（事件关联一致）
    assert agent.seen_ctx[0].task_id == "parent-task"
    assert agent.seen_ctx[0].context_id == "parent-ctx"


async def test_failed_state_raises_and_cleans_up():
    _FakeRemoteA2aAgent.stream = [_event(TaskState.TASK_STATE_FAILED, "remote boom")]

    with pytest.raises(RemoteNodeError, match="failed: remote boom"):
        _ = await _run()

    assert _FakeRemoteA2aAgent.instances[0].cleaned is True


async def test_rejected_state_raises():
    _FakeRemoteA2aAgent.stream = [_event(TaskState.TASK_STATE_REJECTED, "nope")]

    with pytest.raises(RemoteNodeError, match="failed: nope"):
        _ = await _run()


async def test_input_required_raises_explicit_error():
    _FakeRemoteA2aAgent.stream = [_event(TaskState.TASK_STATE_INPUT_REQUIRED, "need info")]

    with pytest.raises(RemoteNodeError, match="resume not supported"):
        _ = await _run()

    assert _FakeRemoteA2aAgent.instances[0].cleaned is True


class _Queue(EventQueue):
    def __init__(self) -> None:
        self.events: list[object] = []

    @override
    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


async def test_events_forwarded_to_parent_stream():
    _FakeRemoteA2aAgent.stream = [
        _event(TaskState.TASK_STATE_WORKING, "working"),
        _event(TaskState.TASK_STATE_COMPLETED, "done"),
    ]
    queue = _Queue()
    output, ic = await _run(event_queue=queue)

    assert output == "done"
    assert len(queue.events) == 2
    assert len(ic.events) == 2
    assert meta(ic.events[0]).author == "reviewer"
