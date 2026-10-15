from __future__ import annotations

from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
)


def build_agent_card(public_url: str) -> AgentCard:
    """Build a minimal AgentCard for the LLMAgent A2A server."""
    base = public_url.rstrip("/")
    return AgentCard(
        name="ChoirWorks Agent",
        description="An LLM agent with tool calling and structured output.",
        version="1.0.0",
        capabilities=AgentCapabilities(streaming=True),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
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
