from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from a2a.server.events import EventQueue

from choirworks.a2a.deps import Deps
from choirworks.a2a.session import SessionManager, SessionRuntime
from choirworks.a2a.state import OrchestrationState
from choirworks.tools.capabilities import ToolEffects

if TYPE_CHECKING:
    from choirworks.a2a.client import RemoteAgentClient
    from choirworks.a2a.registry import AgentRegistry
    from choirworks.core.context import ContextBriefBuilder
    from choirworks.core.llm import LiteLLMClient


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
    """One turn's orchestration context: runtime + deps + tool effects.

    Immutable; shared read-only collaborators live in :class:`Deps` and are
    re-exposed as properties so call sites stay terse.
    """

    runtime: SessionRuntime
    deps: Deps
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

    @property
    def registry(self) -> AgentRegistry:
        return self.deps.registry

    @property
    def remote(self) -> RemoteAgentClient:
        return self.deps.remote

    @property
    def llm(self) -> LiteLLMClient:
        return self.deps.llm

    @property
    def sessions(self) -> SessionManager:
        return self.deps.sessions

    @property
    def config(self) -> ExecutorConfig:
        return self.deps.config

    @property
    def brief_builder(self) -> ContextBriefBuilder:
        return self.deps.brief_builder
