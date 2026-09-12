from datetime import UTC, datetime

import pytest

from agent_hub.core.planner import PlanDraft, validate_plan
from agent_hub.core.policy import PolicyEngine  # noqa: F401  (ensure module import graph sane)
from agent_hub.models.domain import AgentRecord
from agent_hub.sim.llm import SimLLM


def agent(name: str) -> AgentRecord:
    return AgentRecord(
        id=f"id-{name}",
        name=name,
        card_url=f"http://127.0.0.1:1/{name}",
        card={"description": f"{name} agent", "skills": []},
        created_at=datetime.now(UTC),
    )


AGENTS = [agent(name) for name in ("researcher", "writer", "critic", "flaky", "broken")]


def prompt(request: str) -> str:
    lines = "\n".join(f"- {item.name}: {item.card['description']} skills=[]" for item in AGENTS)
    return f"User request:\n{request}\n\nAvailable agents:\n{lines}"


async def plan_for(request: str) -> PlanDraft:
    llm = SimLLM()
    draft = await llm.structured(system="plan", user=prompt(request), schema=PlanDraft)
    validate_plan(draft, AGENTS, 20)
    return draft


def agents_of(draft: PlanDraft) -> list[str]:
    return [node.agent_name for node in draft.nodes]


async def test_default_plan_researches_then_writes():
    draft = await plan_for("帮我调研 A2A 协议并写一份摘要")
    assert agents_of(draft) == ["researcher", "writer"]
    assert draft.nodes[1].deps == [draft.nodes[0].id]


async def test_review_plan_uses_critic():
    draft = await plan_for("帮我评审这段文案")
    assert agents_of(draft) == ["writer", "critic"]
    assert draft.nodes[1].deps == [draft.nodes[0].id]


async def test_retry_plan_uses_flaky_then_writer():
    draft = await plan_for("这个任务可能会偶发失败，请自动重试")
    assert agents_of(draft) == ["flaky", "writer"]


async def test_broken_plan_triggers_replan_without_broken_agent():
    draft = await plan_for("模拟失败并降级替换")
    assert agents_of(draft) == ["broken", "writer"]

    replan_prompt = prompt("模拟失败并降级替换")
    replan_prompt += "\n\nReason for replanning:\nnode 'n1' failed: boom"
    llm = SimLLM()
    replanned = await llm.structured(system="plan", user=replan_prompt, schema=PlanDraft)
    validate_plan(replanned, AGENTS, 20)
    assert "broken" not in agents_of(replanned)
    assert agents_of(replanned) == ["writer"]


async def test_peer_choice_skips_broken_agents():
    from agent_hub.core.orchestrator import PeerChoice

    llm = SimLLM()
    choice = await llm.structured(system="peer", user=prompt("谁来回答？"), schema=PeerChoice)
    assert isinstance(choice, PeerChoice)
    assert choice.agent_name in {"researcher", "writer", "critic", "flaky"}
    assert choice.instruction


async def test_text_returns_nonempty_simulated_answer():
    llm = SimLLM()
    answer = await llm.text(system="assist", user="question: who are you?")
    assert answer.startswith("模拟")
    assert len(answer) > 2


async def test_unknown_schema_rejected():
    llm = SimLLM()

    class Other:
        pass

    with pytest.raises(ValueError):
        await llm.structured(system="x", user="y", schema=Other)  # type: ignore[arg-type]
