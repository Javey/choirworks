from __future__ import annotations

import re
from typing import TypeVar

from pydantic import BaseModel

from agent_hub.core.planner import PlanDraft, PlanNodeDraft

T = TypeVar("T", bound=BaseModel)

FLAKY_AGENTS = {"broken"}

REQUEST_PATTERN = re.compile(r"User request:\n(.*?)(?:\n\nAvailable agents:|\Z)", re.S)
AGENT_PATTERN = re.compile(r"^- (\S+):", re.M)


class SimLLM:
    """离线确定性模拟规划器：实现 LLMClient 协议，无需任何 API Key。"""

    async def structured(self, *, system: str, user: str, schema: type[T]) -> T:
        if schema.__name__ == "PlanDraft":
            return self._plan(user)  # type: ignore[return-value]
        if schema.__name__ == "PeerChoice":
            return schema(  # type: ignore[call-arg]
                agent_name=self._pick(user, preferred=("researcher", "writer", "critic")),
                instruction="请基于已有信息回答 worker agent 的问题。",
            )
        raise ValueError(f"SimLLM 不支持的结构化 schema: {schema.__name__}")

    async def text(self, *, system: str, user: str) -> str:
        return "模拟答复：已收到你的问题，这里给出示例回答。"

    def _plan(self, user: str) -> PlanDraft:
        request = self._request(user)
        if "Reason for replanning:" in user:
            nodes = [
                self._node("n1", "writer", request, deps=[]),
            ]
            return PlanDraft(rationale="模拟重规划：跳过故障节点，直接产出结果", nodes=nodes)
        if any(key in request for key in ("评审", "审查", "确认")):
            nodes = [
                self._node("n1", "writer", request, deps=[]),
                self._node("n2", "critic", f"请评审上一步产出：{request}", deps=["n1"]),
            ]
            return PlanDraft(rationale="模拟计划：先产出再评审", nodes=nodes)
        if any(key in request for key in ("重试", "偶发")):
            nodes = [
                self._node("n1", "flaky", request, deps=[]),
                self._node("n2", "writer", f"根据上一步结果产出最终稿：{request}", deps=["n1"]),
            ]
            return PlanDraft(rationale="模拟计划：偶发失败 + 自动重试", nodes=nodes)
        if any(key in request for key in ("失败", "降级", "替换")):
            nodes = [
                self._node("n1", "broken", request, deps=[]),
                self._node("n2", "writer", f"根据上一步结果产出最终稿：{request}", deps=["n1"]),
            ]
            return PlanDraft(rationale="模拟计划：故障节点 + 降级产出", nodes=nodes)
        nodes = [
            self._node("n1", "researcher", request, deps=[]),
            self._node("n2", "writer", f"基于上一步调研结果撰写：{request}", deps=["n1"]),
        ]
        return PlanDraft(rationale="模拟计划：先调研后撰写", nodes=nodes)

    def _request(self, user: str) -> str:
        match = REQUEST_PATTERN.search(user)
        return (match.group(1).strip() if match else user.strip()) or "模拟任务"

    def _registered(self, user: str) -> set[str]:
        return set(AGENT_PATTERN.findall(user))

    def _pick(self, user: str, preferred: tuple[str, ...]) -> str:
        registered = self._registered(user)
        for name in preferred:
            if name in registered:
                return name
        available = sorted(registered - FLAKY_AGENTS)
        if available:
            return available[0]
        if registered:
            return sorted(registered)[0]
        raise ValueError("SimLLM 无法从提示词中解析出可用 agent")

    def _node(
        self, node_id: str, agent_name: str, text: str, *, deps: list[str]
    ) -> PlanNodeDraft:
        return PlanNodeDraft(
            id=node_id,
            name=agent_name,
            agent_name=agent_name,
            input={"text": text},
            deps=deps,
        )
