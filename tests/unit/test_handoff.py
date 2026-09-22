from __future__ import annotations

from datetime import UTC, datetime

from choirworks.core.context import (
    HANDOFF_MAX_CHARS,
    RECEIPT_CONVENTION,
    build_dispatch_text,
)
from choirworks.core.fencing import QUOTED_CONTENT_BEGIN, QUOTED_CONTENT_END
from choirworks.models.domain import AgentRecord
from choirworks.orchestration.state import NodeState, OrchestrationState, add_member


def make_agent(name: str, description: str = "") -> AgentRecord:
    return AgentRecord(
        id=name,
        name=name,
        card_url=f"http://{name}",
        card={"name": name, "description": description or f"{name} agent", "skills": []},
        created_at=datetime.now(UTC),
    )


def node(node_id: str, **kwargs) -> NodeState:
    return NodeState(
        id=node_id,
        name=node_id,
        agent_name=kwargs.pop("agent_name", node_id),
        agent_url=f"http://{node_id}",
        **kwargs,
    )


def state_with(*nodes: NodeState) -> OrchestrationState:
    state = OrchestrationState(plan_id="plan-1")
    for item in nodes:
        state.nodes[item.id] = item
    return state


def test_dispatch_text_contains_instruction():
    target = node("n2", input_text="撰写报告", deps=["n1"])
    state = state_with(target, node("n1", status="pending"))
    parts = build_dispatch_text(target, state, agents={})
    assert parts[0] == "撰写报告"


def test_dispatch_text_includes_direct_dep_output():
    dep = node("n1", agent_name="researcher", status="completed", output="调研结果")
    target = node("n2", input_text="撰写报告", deps=["n1"])
    state = state_with(dep, target)
    parts = build_dispatch_text(target, state, agents={})
    text = "\n\n".join(parts)
    assert "researcher" in text
    assert "调研结果" in text
    assert QUOTED_CONTENT_BEGIN in text
    assert QUOTED_CONTENT_END in text


def test_dispatch_text_skips_indirect_deps():
    root = node("n0", status="completed", output="根产出")
    dep = node("n1", status="completed", output="中间产出", deps=["n0"])
    target = node("n2", input_text="撰写报告", deps=["n1"])
    state = state_with(root, dep, target)
    parts = build_dispatch_text(target, state, agents={})
    text = "\n\n".join(parts)
    assert "中间产出" in text
    assert "根产出" not in text


def test_dispatch_text_skips_unfinished_deps():
    dep = node("n1", status="working", output="半成品")
    target = node("n2", input_text="撰写报告", deps=["n1"])
    state = state_with(dep, target)
    parts = build_dispatch_text(target, state, agents={})
    text = "\n\n".join(parts)
    assert "半成品" not in text


def test_dispatch_text_truncates_long_dep_output():
    dep = node("n1", status="completed", output="长" * (HANDOFF_MAX_CHARS + 50))
    target = node("n2", input_text="撰写报告", deps=["n1"])
    state = state_with(dep, target)
    parts = build_dispatch_text(target, state, agents={})
    text = "\n\n".join(parts)
    assert "长" * HANDOFF_MAX_CHARS in text
    assert "长" * (HANDOFF_MAX_CHARS + 50) not in text
    assert "已截断" in text


def test_dispatch_text_roster_only_known_members():
    target = node("n2", input_text="撰写报告")
    state = state_with(target)
    add_member(state, "researcher", "http://researcher", "plan")
    add_member(state, "ghost", "http://ghost", "plan")
    parts = build_dispatch_text(
        target,
        state,
        agents={"researcher": make_agent("researcher", "负责调研")},
    )
    text = "\n\n".join(parts)
    assert "researcher" in text
    assert "负责调研" in text
    assert "ghost" not in text


def test_dispatch_text_contains_convention():
    target = node("n1", input_text="开始")
    parts = build_dispatch_text(target, state_with(target), agents={})
    assert RECEIPT_CONVENTION in parts


def test_continuation_text_appends_answer():
    from choirworks.core.context import build_continuation_text

    dep = node("n1", agent_name="researcher", status="completed", output="调研结果")
    target = node("n2", input_text="撰写报告", deps=["n1"])
    state = state_with(dep, target)
    parts = build_continuation_text(
        target,
        state,
        agents={},
        question="预算口径是哪个？",
        answer="按上季度口径",
    )
    assert parts[0] == "撰写报告"
    text = "\n\n".join(parts)
    assert "调研结果" in text
    assert "预算口径是哪个？" in text
    assert "按上季度口径" in text
