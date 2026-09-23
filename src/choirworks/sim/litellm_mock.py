"""Offline simulation at the litellm response layer.

``sim_acompletion`` is a drop-in replacement for ``litellm.acompletion``.
For streaming tool calls it inspects ``tools`` and returns a
``CustomStreamWrapper`` whose chunks carry reasoning text plus the structured
tool-call argument fragments.  Plain calls (``text()``) return a text-only
``ModelResponse``.
"""

from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator
from typing import Any, cast

import litellm
from litellm.litellm_core_utils.redact_messages import LiteLLMLoggingObject
from litellm.litellm_core_utils.streaming_handler import CustomStreamWrapper
from litellm.types.utils import (
    ChatCompletionDeltaToolCall,
    Choices,
    Delta,
    Function,
    Message,
    ModelResponse,
    ModelResponseStream,
    StreamingChoices,
)

from choirworks.core.planner import PlanDraft, PlanNodeDraft

litellm.suppress_debug_info = True

FLAKY_AGENTS = {"auditor"}

REQUEST_PATTERN = re.compile(
    r"User request:\n(.*?)(?:\n\nFor context:|\n\nAvailable agents:|\Z)", re.S
)
AGENT_PATTERN = re.compile(r"^- (\S+):", re.M)
WORKER_PATTERN = re.compile(
    r"Requester: (\S+)\nQuestion / blocked work:\n(.*?)(?:\n\nFor context:|\Z)", re.S
)
PEER_ROUTES = {
    "developer": ("product-manager",),
    "product-manager": ("qa-engineer",),
}
HUMAN_AGENTS = {"code-reviewer"}


class SimLogging:
    """Minimal logging object required by ``CustomStreamWrapper``."""

    model_call_details: dict[str, Any] = {}
    completion_start_time = None
    call_type = "acompletion"
    _is_sync_litellm_request = False
    _llm_caching_handler = None
    _response_cost_calculator = None

    def _update_completion_start_time(self, completion_start_time) -> None:
        self.completion_start_time = completion_start_time

    async def dispatch_success_handlers(self, *args: Any, **kwargs: Any) -> None:
        return None

    def dispatch_failure_handlers(self, *args: Any, **kwargs: Any) -> None:
        return None


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


async def _stream_structured_response(
    reasoning: str, arguments: str, *, tool_name: str, chunk_size: int = 12
) -> AsyncIterator[ModelResponseStream]:
    """Yield reasoning_content chunks then streamed tool-call argument fragments."""
    for start in range(0, len(reasoning), chunk_size):
        yield ModelResponseStream(
            id="sim",
            created=0,
            model="sim",
            choices=[
                StreamingChoices(
                    index=0,
                    delta=Delta(reasoning_content=reasoning[start : start + chunk_size]),
                    finish_reason=None,
                )
            ],
            object="chat.completion.chunk",
        )

    first = True
    for start in range(0, len(arguments), chunk_size):
        call = ChatCompletionDeltaToolCall(
            index=0,
            id="call_1" if first else None,
            type="function" if first else None,
            function=Function(
                name=tool_name if first else None,
                arguments=arguments[start : start + chunk_size],
            ),
        )
        first = False
        yield ModelResponseStream(
            id="sim",
            created=0,
            model="sim",
            choices=[
                StreamingChoices(
                    index=0,
                    delta=Delta(tool_calls=[call]),
                    finish_reason=None,
                )
            ],
            object="chat.completion.chunk",
        )

    yield ModelResponseStream(
        id="sim",
        created=0,
        model="sim",
        choices=[StreamingChoices(index=0, delta=Delta(), finish_reason="tool_calls")],
        object="chat.completion.chunk",
    )


def _request(user: str) -> str:
    match = REQUEST_PATTERN.search(user)
    return (match.group(1).strip() if match else user.strip()) or "任务"


def _registered(user: str) -> set[str]:
    return set(AGENT_PATTERN.findall(user))


def _node(node_id: str, agent_name: str, text: str, *, deps: list[str]) -> PlanNodeDraft:
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

    if request in ("你好", "hello", "hi", "嗨", "在吗"):
        return "用户只是打了个招呼，直接回复即可。", PlanDraft(nodes=[])

    if "Reason for replanning:" in user:
        draft = PlanDraft(
            nodes=[_node("n1", "developer", request, deps=[])],
        )
    elif any(k in request for k in ("重试", "偶发")):
        draft = PlanDraft(
            nodes=[
                _node("n1", "approval-manager", request, deps=[]),
                _node("n2", "finance-analyst", f"根据审批结果生成报告：{request}", deps=["n1"]),
            ],
        )
    elif any(k in request for k in ("失败", "降级", "替换")):
        draft = PlanDraft(
            nodes=[
                _node("n1", "auditor", request, deps=[]),
                _node("n2", "finance-analyst", f"根据审计结果生成报告：{request}", deps=["n1"]),
            ],
        )
    elif any(k in request for k in ("审计", "合规", "风险")):
        draft = PlanDraft(
            nodes=[
                _node("n1", "finance-analyst", request, deps=[]),
                _node("n2", "auditor", f"请审计上一步的财务数据：{request}", deps=["n1"]),
            ],
        )
    elif any(k in request for k in ("审批", "报销", "采购")):
        draft = PlanDraft(
            nodes=[_node("n1", "finance-analyst", request, deps=[])],
        )
    elif any(k in request for k in ("评审", "审查", "确认")):
        draft = PlanDraft(
            nodes=[
                _node("n1", "developer", request, deps=[]),
                _node("n2", "code-reviewer", f"请审查上一步的代码：{request}", deps=["n1"]),
            ],
        )
    elif any(k in request for k in ("协作", "协调", "配合")):
        draft = PlanDraft(
            nodes=[
                _node("n1", "product-manager", request, deps=[]),
                _node("n2", "developer", request, deps=[]),
            ],
        )
    else:
        draft = PlanDraft(
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
    return "\n".join(lines), draft


def _make_assistance_decision(user: str) -> tuple[str, dict[str, Any]]:
    """Return (reasoning_text, OutcomeDecision dict) based on pattern matching."""
    match = WORKER_PATTERN.search(user)
    agents = _registered(user)

    if match is not None:
        asking = match.group(1)
        question = match.group(2).strip()

        if asking in HUMAN_AGENTS:
            reasoning = f"Agent {asking} 的求助需要人工介入。"
            return reasoning, {
                "intent": "need_info",
                "question": "",
                "target_agent": None,
                "instruction": "",
            }

        preferred = PEER_ROUTES.get(asking, ("product-manager", "developer", "qa-engineer"))
        for name in preferred:
            if name in agents and name != asking:
                reasoning = f"Agent {asking} 提出了问题，最适合回答的是 @{name}。"
                return reasoning, {
                    "intent": "need_info",
                    "question": "",
                    "target_agent": name,
                    "instruction": f"请补充信息：{question}",
                }
        available = sorted(agents - FLAKY_AGENTS - {asking})
        if available:
            name = available[0]
            reasoning = f"Agent {asking} 提出了问题，安排 @{name} 回答。"
            return reasoning, {
                "intent": "need_info",
                "question": "",
                "target_agent": name,
                "instruction": f"请补充信息：{question}",
            }

    reasoning = "无可用 peer agent，转人工处理。"
    return reasoning, {
        "intent": "need_info",
        "question": "",
        "target_agent": None,
        "instruction": "",
    }


def _make_outcome_decision() -> tuple[str, dict[str, Any]]:
    """Final replies without a receipt marker are treated as deliverables."""
    return "产出为交付内容。", {
        "intent": "deliver",
        "question": "",
        "target_agent": None,
        "instruction": "",
    }


async def sim_acompletion(
    **kwargs: Any,
) -> ModelResponse | CustomStreamWrapper:
    """Mock ``litellm.acompletion`` for offline simulation.

    Streaming calls return a real ``CustomStreamWrapper``; the forced tool
    name selects the canned reasoning + structured payload.  Non-streaming
    calls return a plain text ``ModelResponse``.
    """
    tools = kwargs.get("tools")
    messages = kwargs.get("messages", [])
    user_content = next(
        (m.get("content", "") for m in reversed(messages) if m.get("role") == "user"),
        "",
    )

    if kwargs.get("stream"):
        tool_name = tools[0]["function"]["name"] if tools else ""
        if tool_name == "create_plan":
            reasoning, draft = _make_plan(user_content)
            arguments = draft.model_dump_json()
        elif tool_name == "OutcomeDecision" and "Agent final reply:" in user_content:
            reasoning, decision = _make_outcome_decision()
            arguments = json.dumps(decision, ensure_ascii=False)
        elif tool_name == "OutcomeDecision":
            reasoning, decision = _make_assistance_decision(user_content)
            arguments = json.dumps(decision, ensure_ascii=False)
        else:
            reasoning, arguments = "已收到你的问题，这里给出示例回答。", ""
        return CustomStreamWrapper(
            completion_stream=_stream_structured_response(
                reasoning, arguments, tool_name=tool_name
            ),
            model="sim",
            logging_obj=cast(LiteLLMLoggingObject, cast(object, SimLogging())),
        )

    # Plain text call
    return _text_response("已收到你的问题，这里给出示例回答。")
