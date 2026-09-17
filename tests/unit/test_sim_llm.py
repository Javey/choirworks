from datetime import UTC, datetime

from choirworks.a2a.executor import AssistanceDecision
from choirworks.core.llm import LiteLLMClient
from choirworks.core.planner import PlanDraft, validate_plan
from choirworks.models.domain import AgentRecord
from choirworks.sim.litellm_mock import sim_acompletion


def agent(name: str) -> AgentRecord:
    return AgentRecord(
        id=f"id-{name}",
        name=name,
        card_url=f"http://127.0.0.1:1/{name}",
        card={"description": f"{name} agent", "skills": []},
        created_at=datetime.now(UTC),
    )


AGENTS = [
    agent(name)
    for name in (
        "product-manager",
        "developer",
        "code-reviewer",
        "qa-engineer",
        "finance-analyst",
        "approval-manager",
        "auditor",
    )
]


def prompt(request: str) -> str:
    lines = "\n".join(f"- {item.name}: {item.card['description']} skills=[]" for item in AGENTS)
    return f"User request:\n{request}\n\nAvailable agents:\n{lines}"


def make_client() -> LiteLLMClient:
    return LiteLLMClient(model="sim", completion_fn=sim_acompletion)


async def plan_for(request: str) -> PlanDraft:
    client = make_client()
    draft = await client.structured(system="plan", user=prompt(request), schema=PlanDraft)
    validate_plan(draft, AGENTS, 20)
    return draft


def agents_of(draft: PlanDraft) -> list[str]:
    return [node.agent_name for node in draft.nodes]


async def test_default_plan_pm_then_developer():
    draft = await plan_for("帮我调研技术方案并写一份设计文档")
    assert agents_of(draft) == ["product-manager", "developer"]
    assert draft.nodes[1].deps == [draft.nodes[0].id]


async def test_review_plan_uses_code_reviewer():
    draft = await plan_for("请审查这段代码的安全性和质量")
    assert agents_of(draft) == ["developer", "code-reviewer"]
    assert draft.nodes[1].deps == [draft.nodes[0].id]


async def test_retry_plan_uses_approval_manager_then_finance_analyst():
    draft = await plan_for("请审批这笔报销，如果失败请重试")
    assert agents_of(draft) == ["approval-manager", "finance-analyst"]


async def test_broken_plan_triggers_replan_without_auditor():
    draft = await plan_for("请审计合规性并降级处理")
    assert agents_of(draft) == ["auditor", "finance-analyst"]

    replan_prompt = prompt("请审计合规性并降级处理")
    replan_prompt += "\n\nReason for replanning:\nnode 'n1' failed: boom"
    client = make_client()
    replanned = await client.structured(system="plan", user=replan_prompt, schema=PlanDraft)
    validate_plan(replanned, AGENTS, 20)
    assert "auditor" not in agents_of(replanned)
    assert agents_of(replanned) == ["developer"]


async def test_assistance_decision_routes_to_pm_for_developer():
    prompt_text = (
        prompt("协作任务")
        + "\n\nRequester: developer\n"
        + "Question / blocked work:\n"
        + "缺少关键信息：请 product-manager 提供需求文档。"
    )
    client = make_client()
    decision = await client.structured(
        system="assistance", user=prompt_text, schema=AssistanceDecision
    )
    assert decision.action == "peer"
    assert decision.agent_name == "product-manager"
    assert "请补充信息" in decision.instruction
    assert "缺少关键信息" in decision.instruction


async def test_assistance_decision_routes_to_qa_for_pm():
    prompt_text = (
        prompt("协作任务")
        + "\n\nRequester: product-manager\n"
        + "Question / blocked work:\n"
        + "需要 qa-engineer 协助确认技术细节。"
    )
    client = make_client()
    decision = await client.structured(
        system="assistance", user=prompt_text, schema=AssistanceDecision
    )
    assert decision.action == "peer"
    assert decision.agent_name == "qa-engineer"
    assert "需要 qa-engineer" in decision.instruction


async def test_assistance_decision_routes_to_human_for_code_reviewer():
    prompt_text = (
        prompt("评审任务")
        + "\n\nRequester: code-reviewer\nQuestion / blocked work:\n需要人工确认评审标准。"
    )
    client = make_client()
    decision = await client.structured(
        system="assistance", user=prompt_text, schema=AssistanceDecision
    )
    assert decision.action == "human"
    assert decision.agent_name is None


async def test_coordination_plan_runs_pm_and_developer_in_parallel():
    draft = await plan_for("请协调团队完成这个功能的需求分析和开发")
    assert agents_of(draft) == ["product-manager", "developer"]
    assert draft.nodes[0].deps == []
    assert draft.nodes[1].deps == []


async def test_raw_response_has_reasoning_content():
    """The mock should populate message.content with reasoning text."""
    client = make_client()
    draft = await client.structured(
        system="plan", user=prompt("帮我调研"), schema=PlanDraft,
    )
    raw = getattr(draft, "_raw_response", None)
    assert raw is not None
    content = raw.choices[0].message.content
    assert "收到请求" in content
    assert "计划" in content


async def test_structured_with_raw_returns_reasoning():
    client = make_client()
    _draft, reasoning = await client.structured_with_raw(
        system="plan", user=prompt("帮我调研"), schema=PlanDraft
    )
    assert "收到请求" in reasoning
    assert "计划" in reasoning
