from __future__ import annotations

from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    AgentSkill,
)

A2A_ROOM_URI = "https://github.com/Javey/choirworks/extensions/room/v1"

_ROOM_DESCRIPTION = (
    "Group-chat fields (mentions, quote, interrupt) in message metadata"
)


def build_agent_card(public_url: str) -> AgentCard:
    base = public_url.rstrip("/")
    return AgentCard(
        name="ChoirWorks",
        description="多 Agent 协作工作群（A2A SDK）",
        version="0.1.0",
        capabilities=AgentCapabilities(
            streaming=True,
            extensions=[
                AgentExtension(
                    uri=A2A_ROOM_URI,
                    description=_ROOM_DESCRIPTION,
                    required=False,
                )
            ],
        ),
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
                protocol_binding="HTTP+JSON",
                url=f"{base}/v1",
                protocol_version="1.0",
            ),
            AgentInterface(
                protocol_binding="JSONRPC",
                url=f"{base}/v1/a2a",
                protocol_version="1.0",
            ),
        ],
    )
