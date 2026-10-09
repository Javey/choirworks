from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, override

from a2a.server.events import EventQueue

from choirworks.core.agents.context import TurnContext
from choirworks.orchestration.session import SessionManager, SessionRuntime
from choirworks.orchestration.state import OrchestrationState

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


class OrchestrationContext(TurnContext):
    """One turn's orchestration context: business subclass of TurnContext.

    Extends the framework base with registry / state / sessions and the
    inbound parse (absorbed ``MessagePayload``).  ``task_id`` /
    ``context_id`` / ``queue`` / ``lock`` delegate to the session runtime so
    long-lived background runners always read the current values (a new
    inbound message updates runtime.task_id / runtime.queue).
    """

    def __init__(
        self,
        *,
        runtime: SessionRuntime,
        registry: AgentRegistry,
        remote: RemoteAgentClient,
        llm: LiteLLMClient,
        sessions: SessionManager,
        config: ExecutorConfig,
        brief_builder: ContextBriefBuilder,
    ) -> None:
        super().__init__(
            task_id=runtime.task_id,
            context_id=runtime.context_id,
            queue=runtime.queue,
            lock=runtime.lock,
            llm=llm,
        )
        self.runtime = runtime
        self.registry = registry
        self.remote = remote
        self.sessions = sessions
        self.config = config
        self.brief_builder = brief_builder

    @property
    def state(self) -> OrchestrationState:
        return self.runtime.state

    @property
    @override
    def task_id(self) -> str:
        return self.runtime.task_id

    @property
    @override
    def context_id(self) -> str:
        return self.runtime.context_id

    @property
    @override
    def queue(self) -> EventQueue:
        return self.runtime.queue

    @property
    @override
    def lock(self) -> asyncio.Lock:
        return self.runtime.lock
