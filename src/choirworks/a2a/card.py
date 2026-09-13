from __future__ import annotations

from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
)


def build_agent_card(public_url: str) -> AgentCard:
    base = public_url.rstrip("/")
    return AgentCard(
        name="ChoirWorks",
        description="多 Agent 协作工作群（A2A facade）",
        version="0.1.0",
        capabilities=AgentCapabilities(streaming=True),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        skills=[
            AgentSkill(
                id="orchestrate",
                name="多 Agent 编排",
                description="规划、调度并协调多个 A2A agent 完成复杂任务",
                tags=["orchestration"],
            )
        ],
        supported_interfaces=[
            AgentInterface(
                protocol_binding="JSONRPC",
                url=f"{base}/v1/a2a",
                protocol_version="1.0",
            )
        ],
    )
