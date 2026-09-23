from dataclasses import replace
from datetime import UTC, datetime

from pydantic import BaseModel

from choirworks.core.llm import LiteLLMClient
from choirworks.core.planner import PlanDraft, validate_plan
from choirworks.models.domain import AgentRecord
from choirworks.sim.litellm_mock import sim_acompletion
from choirworks.tools import FunctionContext, ToolCallResult, create_plan_func
from choirworks.tools.outcome_decision import (
    OutcomeDecision,
    outcome_decision_schema,
    outcome_decision_tool,
)
from tests.support.fakes import FakeRegistry, make_func_ctx


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


def _as[T: BaseModel](item: ToolCallResult, model: type[T]) -> T:
    return (
        item.args if isinstance(item.args, model) else model.model_validate(item.args.model_dump())
    )


async def tool_result(
    client: LiteLLMClient, *, system: str, user: str, tool, ctx: FunctionContext
) -> ToolCallResult:
    result = None
    async for item in client.stream(
        system=system,
        user=user,
        tools=[tool],
        ctx=ctx,
        tool_choice={"type": "function", "function": {"name": tool.name}},
    ):
        if isinstance(item, ToolCallResult):
            result = item
    assert result is not None
    return result


async def plan_for(request: str) -> PlanDraft:
    client = make_client()
    tool = create_plan_func
    ctx = make_func_ctx(FakeRegistry(AGENTS))
    tc = await tool_result(
        client,
        system="plan",
        user=prompt(request),
        tool=tool,
        ctx=ctx,
    )
    draft = _as(tc, PlanDraft)
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
    tool = create_plan_func
    ctx = make_func_ctx(FakeRegistry(AGENTS))
    tc = await tool_result(
        client,
        system="plan",
        user=replan_prompt,
        tool=tool,
        ctx=ctx,
    )
    replanned = _as(tc, PlanDraft)
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
    schema = outcome_decision_schema([a.name for a in AGENTS if a.name != "developer"])
    tool = outcome_decision_tool(schema)
    ctx = make_func_ctx(FakeRegistry(AGENTS))
    tc = await tool_result(
        client,
        system="assistance",
        user=prompt_text,
        tool=tool,
        ctx=ctx,
    )
    decision = _as(tc, OutcomeDecision)
    assert decision.intent == "need_info"
    assert decision.target_agent == "product-manager"
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
    schema = outcome_decision_schema([a.name for a in AGENTS if a.name != "product-manager"])
    tool = outcome_decision_tool(schema)
    ctx = make_func_ctx(FakeRegistry(AGENTS))
    tc = await tool_result(
        client,
        system="assistance",
        user=prompt_text,
        tool=tool,
        ctx=ctx,
    )
    decision = _as(tc, OutcomeDecision)
    assert decision.intent == "need_info"
    assert decision.target_agent == "qa-engineer"
    assert "需要 qa-engineer" in decision.instruction


async def test_assistance_decision_routes_to_human_for_code_reviewer():
    prompt_text = (
        prompt("评审任务")
        + "\n\nRequester: code-reviewer\nQuestion / blocked work:\n需要人工确认评审标准。"
    )
    client = make_client()
    schema = outcome_decision_schema([a.name for a in AGENTS if a.name != "code-reviewer"])
    tool = outcome_decision_tool(schema)
    ctx = make_func_ctx(FakeRegistry(AGENTS))
    tc = await tool_result(
        client,
        system="assistance",
        user=prompt_text,
        tool=tool,
        ctx=ctx,
    )
    decision = _as(tc, OutcomeDecision)
    assert decision.intent == "need_info"
    assert decision.target_agent is None


async def test_coordination_plan_runs_pm_and_developer_in_parallel():
    draft = await plan_for("请协调团队完成这个功能的需求分析和开发")
    assert agents_of(draft) == ["product-manager", "developer"]
    assert draft.nodes[0].deps == []
    assert draft.nodes[1].deps == []


async def test_stream_with_sim_yields_valid_plan():
    client = make_client()
    tool = create_plan_func
    ctx = make_func_ctx(FakeRegistry(AGENTS))
    items = [
        item
        async for item in client.stream(
            system="plan",
            user=prompt("帮我调研技术方案并写一份设计文档"),
            tools=[tool],
            ctx=ctx,
            tool_choice={"type": "function", "function": {"name": "create_plan"}},
        )
    ]
    thinking = "".join(
        getattr(item, "reasoning_content", "") or ""
        for item in items
        if not isinstance(item, ToolCallResult)
    )
    tool_calls = [item for item in items if isinstance(item, ToolCallResult)]
    assert thinking
    assert len(tool_calls) == 1
    draft = tool_calls[0].args
    assert isinstance(draft, PlanDraft)
    validate_plan(draft, AGENTS, 20)
    assert agents_of(draft) == ["product-manager", "developer"]


async def test_greeting_returns_empty_plan():
    draft = await plan_for("你好")
    assert draft.nodes == []


async def test_sim_stream_returns_custom_stream_wrapper():
    from litellm.litellm_core_utils.streaming_handler import CustomStreamWrapper

    result = await sim_acompletion(
        stream=True,
        messages=[{"role": "user", "content": prompt("帮我调研")}],
        tools=[{"type": "function", "function": {"name": "create_plan", "parameters": {}}}],
    )
    assert isinstance(result, CustomStreamWrapper)
    async for _chunk in result:
        pass


async def test_stream_no_tool_call_yields_no_result():
    class Answer(BaseModel):
        value: str

    client = make_client()
    tool = replace(outcome_decision_tool(Answer), name="Answer")  # type: ignore[arg-type]
    ctx = make_func_ctx(FakeRegistry(AGENTS))
    items = [
        item
        async for item in client.stream(
            system="x",
            user="y",
            tools=[tool],
            ctx=ctx,
            tool_choice={"type": "function", "function": {"name": "Answer"}},
        )
    ]
    assert not any(isinstance(item, ToolCallResult) for item in items)
