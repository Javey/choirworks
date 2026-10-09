from __future__ import annotations

import pytest
from a2a.server.agent_execution import RequestContext
from a2a.server.events import EventQueue

from choirworks.core.agents import BaseAgent


class LeafAgent(BaseAgent):
    async def run_async(self, context: RequestContext, event_queue: EventQueue) -> None:
        return None


def test_base_agent_cannot_be_instantiated():
    with pytest.raises(TypeError):
        BaseAgent(name="root")  # type: ignore[abstract]


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
