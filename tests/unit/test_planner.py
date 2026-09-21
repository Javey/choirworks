import json
from datetime import UTC, datetime

import pytest

from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.registry import AgentRegistry
from choirworks.core.llm import LiteLLMClient
from choirworks.core.planner import (
    PlanDraft,
    PlanningFailed,
    PlanNodeDraft,
    PlanValidationError,
    plan,
    validate_plan,
)
from choirworks.models.domain import AgentRecord
from choirworks.store.db import Database
from choirworks.tools import FunctionContext, ToolCallResult
from tests.support.fakes import FakeLLM, make_func_ctx


def make_agent(name: str, skills: list[str]) -> AgentRecord:
    from datetime import UTC, datetime

    return AgentRecord(
        id=name,
        name=name,
        card_url=f"http://{name}",
        card={
            "name": name,
            "description": f"{name} agent",
            "skills": [{"id": s, "name": s, "description": s} for s in skills],
        },
        created_at=datetime.now(UTC),
    )


AGENTS = [make_agent("research", ["search"]), make_agent("writer", ["write"])]


def node(node_id: str, agent: str, deps: list[str] | None = None, skill: str | None = None):
    return PlanNodeDraft(
        id=node_id, name=node_id, agent_name=agent, deps=deps or [], skill_id=skill
    )


def test_valid_plan_passes():
    draft = PlanDraft(
        nodes=[
            node("n1", "research", skill="search"),
            node("n2", "writer", deps=["n1"], skill="write"),
        ],
    )
    validate_plan(draft, AGENTS, max_nodes=10)


def test_unknown_skill_rejected():
    draft = PlanDraft(nodes=[node("n1", "research", skill="nope")])
    with pytest.raises(PlanValidationError, match="unknown skill"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_duplicate_node_id_rejected():
    draft = PlanDraft(nodes=[node("n1", "research"), node("n1", "writer")])
    with pytest.raises(PlanValidationError, match="duplicate node id"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_missing_dependency_rejected():
    draft = PlanDraft(nodes=[node("n1", "research", deps=["n9"])])
    with pytest.raises(PlanValidationError, match="unknown dependency"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_cycle_rejected():
    draft = PlanDraft(
        nodes=[node("n1", "research", deps=["n2"]), node("n2", "writer", deps=["n1"])],
    )
    with pytest.raises(PlanValidationError, match="cycle"):
        validate_plan(draft, AGENTS, max_nodes=10)


def test_too_many_nodes_rejected():
    draft = PlanDraft(nodes=[node(f"n{i}", "research") for i in range(4)])
    with pytest.raises(PlanValidationError, match="too many nodes"):
        validate_plan(draft, AGENTS, max_nodes=3)


def test_empty_plan_accepted():
    draft = PlanDraft(nodes=[])
    validate_plan(draft, AGENTS, max_nodes=10)


async def make_registry(tmp_path, agents):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    now = datetime.now(UTC).isoformat()
    async with db.transaction() as conn:
        for agent in agents:
            await conn.execute(
                "INSERT INTO agent_registry"
                " (id, name, card_url, card, health, last_seen, created_at)"
                " VALUES (?, ?, ?, ?, 'ok', ?, ?)",
                (agent.id, agent.name, agent.card_url, json.dumps(agent.card), now, now),
            )
    return db, remote, registry


async def collect_plan(
    llm: LiteLLMClient,
    registry: AgentRegistry,
    request: str,
    ctx: FunctionContext,
    **kwargs,
):
    chunks: list[str] = []
    tool_call: ToolCallResult | None = None
    async for item in plan(llm, registry, request, ctx=ctx, **kwargs):
        if isinstance(item, ToolCallResult):
            tool_call = item
        else:
            reasoning = getattr(item, "reasoning_content", None) or ""
            content = getattr(item, "content", None) or ""
            if reasoning:
                chunks.append(reasoning)
            if content:
                chunks.append(content)
    assert tool_call is not None
    return "".join(chunks), tool_call


async def test_planner_streams_thinking(tmp_path):
    llm = FakeLLM(
        structured_results=[
            PlanDraft(nodes=[node("n1", "research", skill="search")])
        ]
    )
    db, remote, registry = await make_registry(tmp_path, AGENTS)
    try:
        ctx = make_func_ctx(registry)
        thinking, tool_call = await collect_plan(llm, registry, "研究并写一份报告", ctx)
        assert thinking == "思考：将请求拆解为 1 个节点。"
        draft = tool_call.args
        assert isinstance(draft, PlanDraft)
        assert draft.nodes[0].agent_name == "research"
        assert "Available agents" in llm.stream_calls[0]["user"]
    finally:
        await remote.close()
        await db.close()


async def test_planner_retries_with_feedback(tmp_path):
    bad = PlanDraft(nodes=[node("n1", "research", skill="nope")])
    good = PlanDraft(nodes=[node("n1", "research", skill="search")])
    llm = FakeLLM(structured_results=[bad, good])
    db, remote, registry = await make_registry(tmp_path, AGENTS)
    try:
        ctx = make_func_ctx(registry)
        thinking, tool_call = await collect_plan(llm, registry, "x", ctx)
        assert "unknown skill" in llm.stream_calls[1]["user"]
        assert thinking == "思考：将请求拆解为 1 个节点。" * 2
    finally:
        await remote.close()
        await db.close()


async def test_planner_fails_after_retries(tmp_path):
    bad = PlanDraft(nodes=[node("n1", "research", skill="nope")])
    llm = FakeLLM(structured_results=[bad, bad, bad])
    db, remote, registry = await make_registry(tmp_path, AGENTS)
    try:
        ctx = make_func_ctx(registry)
        with pytest.raises(PlanningFailed):
            await collect_plan(llm, registry, "x", ctx)
        assert len(llm.stream_calls) == 3
    finally:
        await remote.close()
        await db.close()


async def test_planner_rejects_when_no_agents(tmp_path):
    db, remote, registry = await make_registry(tmp_path, [])
    try:
        ctx = make_func_ctx(registry)
        with pytest.raises(PlanningFailed, match="no agents"):
            await collect_plan(FakeLLM(), registry, "x", ctx)
    finally:
        await remote.close()
        await db.close()


async def test_planner_passes_constrained_schema_to_tool(tmp_path):
    llm = FakeLLM(
        structured_results=[
            PlanDraft(nodes=[node("n1", "research", skill="search")])
        ]
    )
    db, remote, registry = await make_registry(tmp_path, AGENTS)
    try:
        ctx = make_func_ctx(registry)
        await collect_plan(llm, registry, "x", ctx)
        call = llm.stream_calls[0]
        tool = call["tools"][0]
        assert tool.name == "create_plan"
        assert "Available agents" in call["user"]
    finally:
        await remote.close()
        await db.close()
