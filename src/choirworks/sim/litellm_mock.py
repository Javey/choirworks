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

FLAKY_AGENTS = {"broken"}

REQUEST_PATTERN = re.compile(r"User request:\n(.*?)(?:\n\nAvailable agents:|\Z)", re.S)
AGENT_PATTERN = re.compile(r"^- (\S+):", re.M)
WORKER_PATTERN = re.compile(
    r"Worker node '(\S+)' asks:\n(.*?)(?:\n\nRegistered agents:|\Z)", re.S
)
PEER_ROUTES = {
    "writer": ("researcher",),
    "researcher": ("analyst",),
}

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
    return (match.group(1).strip() if match else user.strip()) or "模拟任务"


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
            rationale="模拟重规划：跳过故障节点，直接产出结果",
            nodes=[_node("n1", "writer", request, deps=[])],
        )
    elif any(k in request for k in ("评审", "审查", "确认")):
        draft = PlanDraft(
            rationale="模拟计划：先产出再评审",
            nodes=[
                _node("n1", "writer", request, deps=[]),
                _node("n2", "critic", f"请评审上一步产出：{request}", deps=["n1"]),
            ],
        )
    elif any(k in request for k in ("协作", "协调", "配合")):
        draft = PlanDraft(
            rationale="模拟计划：两个 worker 并行协作",
            nodes=[
                _node("n1", "researcher", request, deps=[]),
                _node("n2", "writer", request, deps=[]),
            ],
        )
    elif any(k in request for k in ("重试", "偶发")):
        draft = PlanDraft(
            rationale="模拟计划：偶发失败 + 自动重试",
            nodes=[
                _node("n1", "flaky", request, deps=[]),
                _node("n2", "writer", f"根据上一步结果产出最终稿：{request}", deps=["n1"]),
            ],
        )
    elif any(k in request for k in ("失败", "降级", "替换")):
        draft = PlanDraft(
            rationale="模拟计划：故障节点 + 降级产出",
            nodes=[
                _node("n1", "broken", request, deps=[]),
                _node("n2", "writer", f"根据上一步结果产出最终稿：{request}", deps=["n1"]),
            ],
        )
    else:
        draft = PlanDraft(
            rationale="模拟计划：先调研后撰写",
            nodes=[
                _node("n1", "researcher", request, deps=[]),
                _node("n2", "writer", f"基于上一步调研结果撰写：{request}", deps=["n1"]),
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


def _make_peer_choice(user: str) -> tuple[str, dict[str, Any]]:
    """Return (reasoning_text, PeerChoice dict) based on pattern matching."""
    match = WORKER_PATTERN.search(user)
    agents = _registered(user)

    if match is not None:
        asking = match.group(1)
        question = match.group(2).strip()
        preferred = PEER_ROUTES.get(asking, ("researcher", "writer", "critic", "analyst"))
        for name in preferred:
            if name in agents and name != asking:
                reasoning = f"Worker agent {asking} 提出了问题，最适合回答的是 @{name}。"
                return reasoning, {
                    "agent_name": name,
                    "instruction": f"请补充信息：{question}",
                }
        available = sorted(agents - FLAKY_AGENTS - {asking})
        if available:
            name = available[0]
            reasoning = f"Worker agent {asking} 提出了问题，安排 @{name} 回答。"
            return reasoning, {
                "agent_name": name,
                "instruction": f"请补充信息：{question}",
            }
        reasoning = "没有可用的 peer agent，默认推荐 researcher。"
        return reasoning, {
            "agent_name": "researcher",
            "instruction": "请基于已有信息回答 worker agent 的问题。",
        }

    for name in ("researcher", "writer", "critic"):
        if name in agents:
            reasoning = f"选择 @{name} 来回答 worker agent 的问题。"
            return reasoning, {
                "agent_name": name,
                "instruction": "请基于已有信息回答 worker agent 的问题。",
            }
    available = sorted(agents - FLAKY_AGENTS)
    if available:
        reasoning = f"选择 @{available[0]} 来回答。"
        return reasoning, {
            "agent_name": available[0],
            "instruction": "请基于已有信息回答 worker agent 的问题。",
        }
    reasoning = "无可用 agent，无法做出 peer choice。"
    return reasoning, {
        "agent_name": "researcher",
        "instruction": "请基于已有信息回答 worker agent 的问题。",
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

        if tool_name == "PeerChoice":
            reasoning, choice = _make_peer_choice(user_content)
            return _tool_response(
                "PeerChoice",
                choice,
                content=reasoning,
            )

    # Plain text call
    return _text_response("模拟答复：已收到你的问题，这里给出示例回答。")
