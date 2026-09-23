from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from a2a.server.events import EventQueue

from choirworks.orchestration.session import SessionManager, SessionRuntime
from choirworks.orchestration.state import OrchestrationState
from choirworks.tools.capabilities import ToolEffects

if TYPE_CHECKING:
    from choirworks.a2a.client import RemoteAgentClient
    from choirworks.core.context import ContextBriefBuilder
    from choirworks.core.llm import LiteLLMClient
    from choirworks.orchestration.registry import AgentRegistry


@dataclass(frozen=True, slots=True)
class ExecutorConfig:
    max_parallel: int = 5
    node_timeout: float = 600.0
    max_node_attempts: int = 2
    retry_backoff: float = 1.0
    max_derived_nodes: int = 5
    max_revisions: int = 3
    replan_on_failure: bool = True
    max_nodes: int = 20
    max_plan_retries: int = 2


@dataclass(frozen=True, slots=True)
class OrchestrationContext:
    """One turn's orchestration context: session runtime + collaborators + effects.

    Immutable; the long-lived collaborators are held directly, so call sites
    read ``ctx.registry`` / ``ctx.llm`` without an extra layer, and the
    session runtime's hot fields are re-exposed as properties.
    """

    runtime: SessionRuntime
    registry: AgentRegistry
    remote: RemoteAgentClient
    llm: LiteLLMClient
    sessions: SessionManager
    config: ExecutorConfig
    brief_builder: ContextBriefBuilder
    effects: ToolEffects

    @property
    def state(self) -> OrchestrationState:
        return self.runtime.state

    @property
    def task_id(self) -> str:
        return self.runtime.task_id

    @property
    def context_id(self) -> str:
        return self.runtime.context_id

    @property
    def queue(self) -> EventQueue:
        return self.runtime.queue

    @property
    def lock(self) -> asyncio.Lock:
        return self.runtime.lock
