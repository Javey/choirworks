from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from a2a.server.events import EventQueue

    from choirworks.a2a.registry import AgentRegistry
    from choirworks.a2a.state import OrchestrationState
    from choirworks.core.llm import LiteLLMClient


@dataclass
class ExecutorConfig:
    max_parallel: int = 5
    node_timeout: float = 600.0
    max_node_attempts: int = 2
    retry_backoff: float = 1.0
    max_derived_nodes: int = 5
    max_revisions: int = 3
    replan_on_failure: bool = True
    max_nodes: int = 20


@dataclass
class OrchestrationContext:
    runtime: object
    registry: AgentRegistry
    llm: LiteLLMClient
    config: ExecutorConfig
    emitter: object
    session_mgr: object
    executor: object | None = None

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
    def lock(self) -> object:
        return self.runtime.lock
