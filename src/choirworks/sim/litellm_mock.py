"""Offline simulation at the litellm response layer.

``sim_acompletion`` is a drop-in replacement for ``litellm.acompletion``.
It inspects the ``tools`` argument that *instructor* injects to determine
which structured schema is requested, then returns a ``ModelResponse``
whose ``message.content`` carries reasoning text and whose
``message.tool_calls`` carries the structured payload — exactly what a
real LLM in TOOLS mode would produce.

For plain ``text()`` calls (no ``tools``), it returns a text-only response.
"""

from __future__ import annotations

import json
import re
from typing import Any

from litellm.types.utils import (
    ChatCompletionMessageToolCall,
    Choices,
    Message,
    ModelResponse,
)

from choirworks.core.planner import PlanDraft, PlanNodeDraft

FLAKY_AGENTS = {"auditor"}

REQUEST_PATTERN = re.compile(r"User request:\n(.*?)(?:\n\nAvailable agents:|\Z)", re.S)
AGENT_PATTERN = re.compile(r"^- (\S+):", re.M)
WORKER_PATTERN = re.compile(
    r"Requester: (\S+)\nQuestion / blocked work:\n(.*?)(?:\n\nFor context:|\Z)", re.S
)
PEER_ROUTES = {
    "developer": ("product-manager",),
    "product-manager": ("qa-engineer",),
}
HUMAN_AGENTS = {"code-reviewer"}

_NEXT_ID = 0


def _next_id() -> str:
    global _NEXT_ID
    _NEXT_ID += 1
    return f"call_{_NEXT_ID}"


def _tool_response(
    tool_name: str, arguments: dict[str, Any], *, content: str
) -> ModelResponse:
    """Build a TOOLS-mode ModelResponse with reasoning text + tool_call."""
    tool_call = ChatCompletionMessageToolCall(
        id=_next_id(),
        type="function",
        function={
            "name": tool_name,
            "arguments": json.dumps(arguments, ensure_ascii=False),
        },
    )
    msg = Message(content=content, role="assistant", tool_calls=[tool_call])
    return ModelResponse(
        id="sim",
        created=0,
        model="sim",
        choices=[Choices(finish_reason="tool_calls", index=0, message=msg)],
        object="chat.completion",
    )


def _text_response(content: str) -> ModelResponse:
    """Build a plain text ModelResponse (no tool_calls)."""
    msg = Message(content=content, role="assistant")
    return ModelResponse(
        id="sim",
        created=0,
        model="sim",
        choices=[Choices(finish_reason="stop", index=0, message=msg)],
        object="chat.completion",
    )


def _request(user: str) -> str:
    match = REQUEST_PATTERN.search(user)
    return (match.group(1).strip() if match else user.strip()) or "任务"


def _registered(user: str) -> set[str]:
    return set(AGENT_PATTERN.findall(user))


def _node(
    node_id: str, agent_name: str, text: str, *, deps: list[str]
) -> PlanNodeDraft:
    return PlanNodeDraft(
        id=node_id,
        name=agent_name,
        agent_name=agent_name,
        input={"text": text},
        deps=deps,
    )


def _make_plan(user: str) -> tuple[str, PlanDraft]:
    """Return (reasoning_text, PlanDraft) based on pattern matching."""
    request = _request(user)
    agents = sorted(_registered(user))
    agents_text = ", ".join(agents) if agents else "(无)"

    if "Reason for replanning:" in user:
        draft = PlanDraft(
            rationale="重规划：跳过故障节点，直接产出结果",
            nodes=[_node("n1", "developer", request, deps=[])],
        )
    elif any(k in request for k in ("重试", "偶发")):
        draft = PlanDraft(
            rationale="计划：审批流程（偶发超时需重试）",
            nodes=[
                _node("n1", "approval-manager", request, deps=[]),
                _node("n2", "finance-analyst", f"根据审批结果生成报告：{request}", deps=["n1"]),
            ],
        )
    elif any(k in request for k in ("失败", "降级", "替换")):
        draft = PlanDraft(
            rationale="计划：审计流程（可能失败需重规划）",
            nodes=[
                _node("n1", "auditor", request, deps=[]),
                _node("n2", "finance-analyst", f"根据审计结果生成报告：{request}", deps=["n1"]),
            ],
        )
    elif any(k in request for k in ("审计", "合规", "风险")):
        draft = PlanDraft(
            rationale="计划：财务分析后进行合规审计",
            nodes=[
                _node("n1", "finance-analyst", request, deps=[]),
                _node("n2", "auditor", f"请审计上一步的财务数据：{request}", deps=["n1"]),
            ],
        )
    elif any(k in request for k in ("审批", "报销", "采购")):
        draft = PlanDraft(
            rationale="计划：财务数据分析",
            nodes=[_node("n1", "finance-analyst", request, deps=[])],
        )
    elif any(k in request for k in ("评审", "审查", "确认")):
        draft = PlanDraft(
            rationale="计划：先开发再代码审查",
            nodes=[
                _node("n1", "developer", request, deps=[]),
                _node("n2", "code-reviewer", f"请审查上一步的代码：{request}", deps=["n1"]),
            ],
        )
    elif any(k in request for k in ("协作", "协调", "配合")):
        draft = PlanDraft(
            rationale="计划：产品经理与开发并行推进",
            nodes=[
                _node("n1", "product-manager", request, deps=[]),
                _node("n2", "developer", request, deps=[]),
            ],
        )
    else:
        draft = PlanDraft(
            rationale="计划：先需求分析后开发",
            nodes=[
                _node("n1", "product-manager", request, deps=[]),
                _node("n2", "developer", f"基于需求分析结果进行开发：{request}", deps=["n1"]),
            ],
        )

    lines = [
        f"收到请求：「{request}」",
        f"已注册的 agent 有：{agents_text}",
        "分析需求后，制定以下计划：",
    ]
    for n in draft.nodes:
        dep_text = f"（依赖 {', '.join(n.deps)}）" if n.deps else ""
        lines.append(f"  • {n.id} → @{n.agent_name} 执行「{n.name}」{dep_text}")
    lines.append(f"理由：{draft.rationale}")
    return "\n".join(lines), draft


def _make_assistance_decision(user: str) -> tuple[str, dict[str, Any]]:
    """Return (reasoning_text, AssistanceDecision dict) based on pattern matching."""
    match = WORKER_PATTERN.search(user)
    agents = _registered(user)

    if match is not None:
        asking = match.group(1)
        question = match.group(2).strip()

        if asking in HUMAN_AGENTS:
            reasoning = f"Agent {asking} 的求助需要人工介入。"
            return reasoning, {
                "action": "human",
                "agent_name": None,
                "instruction": "",
            }

        preferred = PEER_ROUTES.get(asking, ("product-manager", "developer", "qa-engineer"))
        for name in preferred:
            if name in agents and name != asking:
                reasoning = f"Agent {asking} 提出了问题，最适合回答的是 @{name}。"
                return reasoning, {
                    "action": "peer",
                    "agent_name": name,
                    "instruction": f"请补充信息：{question}",
                }
        available = sorted(agents - FLAKY_AGENTS - {asking})
        if available:
            name = available[0]
            reasoning = f"Agent {asking} 提出了问题，安排 @{name} 回答。"
            return reasoning, {
                "action": "peer",
                "agent_name": name,
                "instruction": f"请补充信息：{question}",
            }

    reasoning = "无可用 peer agent，转人工处理。"
    return reasoning, {
        "action": "human",
        "agent_name": None,
        "instruction": "",
    }


async def sim_acompletion(**kwargs: Any) -> ModelResponse:
    """Mock ``litellm.acompletion`` for offline simulation.

    Detects the requested schema via ``tools`` (injected by instructor)
    or falls back to plain text response.
    """
    tools = kwargs.get("tools")
    messages = kwargs.get("messages", [])
    user_content = ""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            user_content = msg.get("content", "")
            break

    if tools:
        tool_name = tools[0]["function"]["name"]

        if tool_name == "PlanDraft":
            reasoning, draft = _make_plan(user_content)
            return _tool_response(
                "PlanDraft",
                draft.model_dump(),
                content=reasoning,
            )

        if tool_name == "AssistanceDecision":
            reasoning, decision = _make_assistance_decision(user_content)
            return _tool_response(
                "AssistanceDecision",
                decision,
                content=reasoning,
            )

    # Plain text call
    return _text_response("已收到你的问题，这里给出示例回答。")
