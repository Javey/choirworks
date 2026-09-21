from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from choirworks.a2a.client import RemoteAgentClient
    from choirworks.a2a.context import ExecutorConfig
    from choirworks.a2a.registry import AgentRegistry
    from choirworks.a2a.session import SessionManager
    from choirworks.core.context import ContextBriefBuilder
    from choirworks.core.llm import LiteLLMClient


@dataclass(frozen=True, slots=True)
class Deps:
    """Long-lived collaborators every orchestration function may need.

    Carried by :class:`~choirworks.a2a.context.OrchestrationContext`, so
    functions receive one context object instead of constructor-injected
    manager instances.
    """

    registry: AgentRegistry
    remote: RemoteAgentClient
    llm: LiteLLMClient
    sessions: SessionManager
    config: ExecutorConfig
    brief_builder: ContextBriefBuilder
