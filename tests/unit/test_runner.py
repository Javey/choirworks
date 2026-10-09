from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from types import SimpleNamespace

from a2a.server.events import EventQueue

from choirworks.core.agents.base import BaseAgent
from choirworks.core.agents.context import TurnContext
from choirworks.core.runner import Runner


@dataclass(slots=True)
class _LeafAgent(BaseAgent):
    calls: list[tuple[str, bool]] = field(default_factory=list)

    async def run_async(self, ctx: TurnContext, user: str, **tool_kwargs: object) -> None:
        self.calls.append((user, ctx.lock.locked()))


def _make_ctx(queue: EventQueue) -> TurnContext:
    return TurnContext(
        task_id="t1",
        context_id="c1",
        queue=queue,
        lock=asyncio.Lock(),
        llm=None,  # type: ignore[arg-type]
    )


def _request(text: str) -> SimpleNamespace:
    return SimpleNamespace(get_user_input=lambda: text)


async def test_runner_prepares_with_request_and_event_queue_and_drives_root():
    queue = EventQueue()
    ctx = _make_ctx(queue)
    agent = _LeafAgent(name="root")
    seen: list[tuple[object, object]] = []

    async def prepare(context: object, event_queue: object) -> TurnContext:
        seen.append((context, event_queue))
        return ctx

    context = _request(" 你好 ")
    runner = Runner(root=agent, prepare=prepare)  # type: ignore[arg-type]

    await runner.run_async(context, queue)  # type: ignore[arg-type]

    assert seen == [(context, queue)]
    assert agent.calls == [("你好", True)]
    assert not ctx.lock.locked()
