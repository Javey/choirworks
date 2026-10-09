from __future__ import annotations

from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    AgentSkill,
)

from choirworks.a2a.room import A2A_ROOM_URI
from choirworks.core.events import A2A_CW_EVENTS_URI

_ROOM_DESCRIPTION = "Group-chat fields (mentions, quote, interrupt) in message metadata"
_EVENTS_DESCRIPTION = (
    "Custom content kinds in part metadata "
    "(cw_type: thought | text | function_call | question); "
    "plan state deltas ride on status event metadata (cw_delta)"
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
                ),
                AgentExtension(
                    uri=A2A_CW_EVENTS_URI,
                    description=_EVENTS_DESCRIPTION,
                    required=False,
                ),
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
