from datetime import UTC, datetime

from choirworks.core.fencing import (
    QUOTED_CONTENT_BEGIN,
    QUOTED_CONTENT_END,
    QUOTED_CONTENT_PREAMBLE,
)
from choirworks.core.planner import SYSTEM_PROMPT
from choirworks.models.domain import AgentRecord
from choirworks.orchestration.prompts import (
    build_assist_input,
    build_assistance_decision_user,
    build_peer_fallback_input,
    build_planner_capabilities,
    build_planner_user_message,
    build_replan_reason,
)
from choirworks.orchestration.state import NodeState
from choirworks.subagents.assistance.prompt import ASSISTANCE_SYSTEM
from choirworks.subagents.outcome.prompt import OUTCOME_SYSTEM
from choirworks.subagents.repair.prompt import REPAIR_SYSTEM


def make_agent(name: str, description: str = "", skills: list[str] | None = None):
    return AgentRecord(
        id=name,
        name=name,
        card_url=f"http://{name}",
        card={
            "name": name,
            "description": description or f"{name} agent",
            "skills": [{"id": s, "name": s, "description": s} for s in (skills or [])],
        },
        created_at=datetime.now(UTC),
    )


def node(node_id: str, **kwargs) -> NodeState:
    return NodeState(
        id=node_id,
        name=node_id,
        agent_name=kwargs.pop("agent_name", node_id),
        **kwargs,
    )


# ------------------------------------------------------------- builders


def test_own_system_prompts_do_not_include_quote_preamble():
    for prompt in (SYSTEM_PROMPT, OUTCOME_SYSTEM, ASSISTANCE_SYSTEM, REPAIR_SYSTEM):
        assert QUOTED_CONTENT_PREAMBLE not in prompt


def test_build_planner_capabilities_caps_description():
    agents = [make_agent("a", description="x" * 1100, skills=["s1"])]
    text = build_planner_capabilities(agents)
    assert text.startswith("- a: ")
    assert "... [truncated]" in text
    assert "s1 (s1)" in text


def test_build_planner_user_message_fences_untrusted_blocks():
    user = build_planner_user_message(
        "- a: agent",
        reason="nodes failed: boom",
    )
    assert QUOTED_CONTENT_PREAMBLE in user
    assert user.count(QUOTED_CONTENT_BEGIN) == 3
    assert user.count(QUOTED_CONTENT_END) == 3
    assert "Available agents:\n- a: agent" in user
    assert "Reason for replanning:\nnodes failed: boom" in user


def test_build_planner_user_message_without_reason():
    user = build_planner_user_message("- a: agent")
    assert user.count(QUOTED_CONTENT_BEGIN) == 2
    assert "Reason for replanning" not in user


def test_build_assistance_decision_user():
    candidates = [make_agent("a"), make_agent("b")]
    user = build_assistance_decision_user("c", "需要确认", candidates)
    assert "Requester: c" in user
    assert "Question / blocked work:\n需要确认" in user
    assert QUOTED_CONTENT_PREAMBLE in user
    assert "- a: a agent" in user
    assert "- b: b agent" in user


def test_build_replan_reason():
    nodes = [
        node("n1", status="completed", output="ok" * 150),
        node("n2", status="failed", error="boom"),
        node("n3", status="failed"),
    ]
    assert build_replan_reason(nodes) == "nodes failed: boom, n3"


def test_build_assist_input_and_peer_fallback():
    assist = build_assist_input("a", "产出")
    assert "a 在协作中请求你的协助。" in assist
    assert QUOTED_CONTENT_PREAMBLE in assist
    assert QUOTED_CONTENT_BEGIN in assist
    assert build_peer_fallback_input("问题") == "请协助回答以下问题：\n问题"
