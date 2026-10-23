from __future__ import annotations

import pytest
from pydantic import ValidationError

from new_choirworks.core.agents.llm_agent import LLMAgent
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.plan import DagPlan
from new_choirworks.orchestrator.planner import (
    build_planner,
    build_planner_brief,
    build_planner_instruction,
    constrained_plan_schema,
)
from new_choirworks.orchestrator.runtime import DagRuntime

from ..conftest import FakeLLM

AGENTS = [
    AgentInfo(name="writer", url="http://writer", description="Writes text"),
    AgentInfo(name="reviewer", url="http://reviewer", description="Reviews text"),
]


def _schema() -> type[DagPlan]:
    return constrained_plan_schema([a.name for a in AGENTS])


def _node_dict(nid: str, agent: str) -> dict[str, object]:
    return {"id": nid, "name": nid, "agent_name": agent, "instruction": "do", "deps": []}


def test_empty_catalog_rejected():
    """P1-4：空 agent 目录给出明确配置错误。"""
    with pytest.raises(ValueError, match="agent catalog is empty"):
        _ = constrained_plan_schema([])
    with pytest.raises(ValueError, match="agent catalog is empty"):
        _ = build_planner([], FakeLLM(responder=lambda _i, _r: []))


def test_schema_accepts_registered_agent():
    plan = _schema().model_validate({"nodes": [_node_dict("a", "writer")]})
    assert plan.nodes[0].agent_name == "writer"


def test_schema_rejects_hallucinated_agent():
    with pytest.raises(ValidationError):
        _ = _schema().model_validate({"nodes": [_node_dict("a", "ghost")]})


def test_instruction_lists_agents_and_rules():
    text = build_planner_instruction(AGENTS)
    assert "- writer: Writes text" in text
    assert "- reviewer: Reviews text" in text
    assert "{{output:<node_id>}}" in text
    assert "self-contained" in text
    assert "Empty nodes list" in text


def test_build_planner_returns_llm_agent_with_schema():
    planner = build_planner(AGENTS, FakeLLM(responder=lambda _i, _r: []))
    assert isinstance(planner, LLMAgent)
    assert planner.include_contents == "none"
    assert planner.output_schema is not None


def test_planner_brief_contains_task_plan_and_outputs():
    runtime = DagRuntime(agents={"writer": AGENTS[0]}, user_task="analyze sales")
    runtime.init_plan(DagPlan.model_validate({"nodes": [_node_dict("a", "writer")]}))
    runtime.record_completion("a", "result-1")

    brief = build_planner_brief(runtime)

    assert "analyze sales" in brief
    assert "status=NodeStatus.COMPLETED" in brief or "status=" in brief
    assert "[a output]" in brief
    assert "result-1" in brief
