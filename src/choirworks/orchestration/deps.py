from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from choirworks.a2a.client import RemoteAgentClient
    from choirworks.core.context import ContextBriefBuilder
    from choirworks.core.llm import LiteLLMClient
    from choirworks.orchestration.context import ExecutorConfig
    from choirworks.orchestration.registry import AgentRegistry
    from choirworks.orchestration.session import SessionManager


@dataclass(frozen=True, slots=True)
class Deps:
    """Long-lived collaborators every orchestration function may need.

    Carried by :class:`~choirworks.orchestration.context.OrchestrationContext`, so
    functions receive one context object instead of constructor-injected
    manager instances.
    """

    registry: AgentRegistry
    remote: RemoteAgentClient
    llm: LiteLLMClient
    sessions: SessionManager
    config: ExecutorConfig
    brief_builder: ContextBriefBuilder
