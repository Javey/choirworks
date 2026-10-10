from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from types import SimpleNamespace

import pytest
from a2a.server.events import EventQueue

from choirworks.core.agents.base import BaseAgent, run_agent
from choirworks.core.agents.context import TurnContext
from choirworks.core.events import AgentEvent, ResultEvent


def make_ctx() -> TurnContext:
    return SimpleNamespace(  # type: ignore[return-value]
        task_id="t1",
        context_id="c1",
        queue=EventQueue(),
        lock=asyncio.Lock(),
        llm=None,
        messages=[],
    )


class LeafAgent(BaseAgent):
    async def run_async(
        self, ctx: TurnContext, user: str, **tool_kwargs: object
    ) -> AsyncGenerator[AgentEvent]:
        yield ResultEvent(None)


def test_base_agent_cannot_be_instantiated():
    with pytest.raises(TypeError):
        BaseAgent(name="root")  # type: ignore[abstract]


async def test_leaf_agent_runs_one_turn():
    agent = LeafAgent(name="leaf")

    result = await run_agent(agent, make_ctx(), "hi")

    assert result is None


def test_sub_agent_parent_backref():
    child = LeafAgent(name="child")
    parent = LeafAgent(name="parent", sub_agents=[child])

    assert child.parent_agent is parent
    assert parent.parent_agent is None


def test_sub_agent_can_only_have_one_parent():
    child = LeafAgent(name="child")
    LeafAgent(name="first", sub_agents=[child])

    with pytest.raises(ValueError, match="already has a parent agent"):
        LeafAgent(name="second", sub_agents=[child])


def test_defaults():
    agent = LeafAgent(name="a")

    assert agent.description == ""
    assert agent.sub_agents == []
    assert agent.parent_agent is None
