from __future__ import annotations

from collections.abc import AsyncGenerator

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.graph import DEFAULT_ROUTE
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.helper import Helper, parse_mentions
from new_choirworks.orchestrator.plan import DagPlan, PlanNode
from new_choirworks.orchestrator.runtime import MAX_PLAN_NODES, DagRuntime, NodeStatus

AGENTS = {
    "writer": AgentInfo(name="writer", url="http://writer"),
    "reviewer": AgentInfo(name="reviewer", url="http://reviewer"),
    "editor": AgentInfo(name="editor", url="http://editor"),
}
AGENT_NAMES = {"writer", "reviewer", "editor"}


def _runtime_with_output(output: str) -> DagRuntime:
    rt = DagRuntime(agents=AGENTS)
    rt.init_plan(
        DagPlan(nodes=[PlanNode(id="n1", name="n1", agent_name="writer", instruction="write")])
    )
    rt.record_completion("n1", output)
    return rt


async def _run(rt: DagRuntime, node_input: object | None) -> Context:
    helper = Helper(name="helper", runtime=rt)
    ctx = Context(invocation_context=InvocationContext(), node=helper)
    agen: AsyncGenerator[A2AEvent, None] = helper.run(ctx, node_input=node_input)
    async for _ in agen:
        pass
    return ctx


# --- parse_mentions ---


def test_parse_mentions_single():
    assert parse_mentions("@reviewer check chapter 3", AGENT_NAMES) == [
        ("reviewer", "check chapter 3")
    ]


def test_parse_mentions_multiple():
    mentions = parse_mentions("@writer rewrite @reviewer check result", AGENT_NAMES)
    assert ("writer", "rewrite") in mentions
    assert ("reviewer", "check result") in mentions


def test_parse_mentions_ignores_unknown_agent():
    assert parse_mentions("@unknown do something", AGENT_NAMES) == []


def test_parse_mentions_ignores_no_instruction():
    assert parse_mentions("@reviewer", AGENT_NAMES) == []


def test_parse_mentions_no_mentions():
    assert parse_mentions("all done, no help needed", AGENT_NAMES) == []


def test_parse_mentions_empty_text():
    assert parse_mentions("", AGENT_NAMES) == []


# --- Helper node ---


async def test_helper_mention_adds_assist_and_wires_parent():
    rt = _runtime_with_output("@reviewer please review the draft")
    ctx = await _run(rt, node_input="n1")

    assert ctx.route == "execute"
    parent = rt.get_node("n1")
    assert len(parent.deps) == 1
    assist_id = parent.deps[0]
    assist = rt.get_node(assist_id)
    assert assist.agent_name == "reviewer"
    assert assist.assist_for == "n1"
    assert assist.deps == []
    assert rt.node_status["n1"] == NodeStatus.WAITING_ASSIST
    assert rt.node_status[assist_id] == NodeStatus.PENDING


async def test_helper_without_mention_routes_default():
    rt = _runtime_with_output("Task completed successfully")
    ctx = await _run(rt, node_input="n1")
    assert ctx.route == DEFAULT_ROUTE


async def test_helper_unknown_agent_mention_routes_default():
    rt = _runtime_with_output("@ghost please help")
    ctx = await _run(rt, node_input="n1")
    assert ctx.route == DEFAULT_ROUTE


async def test_helper_multiple_mentions_adds_multiple_nodes():
    rt = _runtime_with_output("@reviewer check it @editor trim it")
    ctx = await _run(rt, node_input="n1")

    assert ctx.route == "execute"
    parent = rt.get_node("n1")
    assert len(parent.deps) == 2
    agents = {rt.get_node(pid).agent_name for pid in parent.deps}
    assert agents == {"reviewer", "editor"}


async def test_helper_missing_input_routes_default():
    rt = DagRuntime(agents=AGENTS)
    ctx = await _run(rt, node_input=None)
    assert ctx.route == DEFAULT_ROUTE


async def test_helper_self_mention_routes_default():
    rt = _runtime_with_output("@writer help me please")
    ctx = await _run(rt, node_input="n1")
    assert ctx.route == DEFAULT_ROUTE


async def test_helper_duplicate_assist_routes_default():
    rt = _runtime_with_output("@reviewer check this")
    _ = await _run(rt, node_input="n1")
    ctx = await _run(rt, node_input="n1")
    assert ctx.route == DEFAULT_ROUTE


async def test_helper_over_limit_routes_default():
    rt = _runtime_with_output("@reviewer check this")
    for i in range(MAX_PLAN_NODES):
        rt.plan.nodes.append(
            PlanNode(id=f"x{i}", name=f"x{i}", agent_name="writer", instruction="x")
        )
    ctx = await _run(rt, node_input="n1")
    assert ctx.route == DEFAULT_ROUTE


async def test_helper_removed_node_routes_default():
    rt = _runtime_with_output("@reviewer check this")
    rt.plan.nodes.clear()
    ctx = await _run(rt, node_input="n1")
    assert ctx.route == DEFAULT_ROUTE


def test_parse_mentions_requires_space_after_name():
    """语法契约：名后必须空格，换行/标点不命中（P2-3 文档化）。"""
    assert parse_mentions("@reviewer\n请检查", AGENT_NAMES) == []
    assert parse_mentions("@reviewer,请检查", AGENT_NAMES) == []
    assert parse_mentions("mail me at a@b.com", AGENT_NAMES) == []
