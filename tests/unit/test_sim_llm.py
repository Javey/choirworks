from datetime import UTC, datetime

import pytest
from pydantic import BaseModel

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


async def structured_result(client: LiteLLMClient, *, system: str, user: str, schema, tool_name):
    result = None
    async for item in client.stream_structured(
        system=system, user=user, schema=schema, tool_name=tool_name
    ):
        if not isinstance(item, str):
            result = item
    return result


async def plan_for(request: str) -> PlanDraft:
    client = make_client()
    draft = await structured_result(
        client, system="plan", user=prompt(request), schema=PlanDraft, tool_name="PlanDraft"
    )
    assert draft is not None
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
    replanned = await structured_result(
        client, system="plan", user=replan_prompt, schema=PlanDraft, tool_name="PlanDraft"
    )
    assert replanned is not None
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
    decision = await structured_result(
        client,
        system="assistance",
        user=prompt_text,
        schema=AssistanceDecision,
        tool_name="AssistanceDecision",
    )
    assert decision is not None
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
    decision = await structured_result(
        client,
        system="assistance",
        user=prompt_text,
        schema=AssistanceDecision,
        tool_name="AssistanceDecision",
    )
    assert decision is not None
    assert decision.action == "peer"
    assert decision.agent_name == "qa-engineer"
    assert "需要 qa-engineer" in decision.instruction


async def test_assistance_decision_routes_to_human_for_code_reviewer():
    prompt_text = (
        prompt("评审任务")
        + "\n\nRequester: code-reviewer\nQuestion / blocked work:\n需要人工确认评审标准。"
    )
    client = make_client()
    decision = await structured_result(
        client,
        system="assistance",
        user=prompt_text,
        schema=AssistanceDecision,
        tool_name="AssistanceDecision",
    )
    assert decision is not None
    assert decision.action == "human"
    assert decision.agent_name is None


async def test_coordination_plan_runs_pm_and_developer_in_parallel():
    draft = await plan_for("请协调团队完成这个功能的需求分析和开发")
    assert agents_of(draft) == ["product-manager", "developer"]
    assert draft.nodes[0].deps == []
    assert draft.nodes[1].deps == []


async def test_stream_structured_with_sim_yields_valid_plan():
    client = make_client()
    items = [
        item
        async for item in client.stream_structured(
            system="plan",
            user=prompt("帮我调研技术方案并写一份设计文档"),
            schema=PlanDraft,
            tool_name="PlanDraft",
        )
    ]
    thinking = "".join(item for item in items if isinstance(item, str))
    drafts = [item for item in items if isinstance(item, PlanDraft)]
    assert thinking
    assert len(drafts) == 1
    validate_plan(drafts[0], AGENTS, 20)
    assert agents_of(drafts[0]) == ["product-manager", "developer"]


async def test_sim_stream_returns_custom_stream_wrapper():
    from litellm.litellm_core_utils.streaming_handler import CustomStreamWrapper

    result = await sim_acompletion(
        stream=True,
        messages=[{"role": "user", "content": prompt("帮我调研")}],
        tools=[
            {"type": "function", "function": {"name": "PlanDraft", "parameters": {}}}
        ],
    )
    assert isinstance(result, CustomStreamWrapper)
    async for _chunk in result:
        pass


async def test_stream_structured_raises_without_tool_call():
    class Answer(BaseModel):
        value: str

    client = make_client()
    with pytest.raises(ValueError, match="did not call"):
        [
            item
            async for item in client.stream_structured(
                system="x", user="y", schema=Answer, tool_name="Answer"
            )
        ]
