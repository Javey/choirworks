from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from a2a.server.events import EventQueue

from choirworks.a2a.registry import AgentRegistry
from choirworks.a2a.session import SessionManager, SessionRuntime
from choirworks.a2a.state import OrchestrationState
from choirworks.core.llm import LiteLLMClient

if TYPE_CHECKING:
    from choirworks.a2a.executor import ChoirWorksAgentExecutor


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
    max_plan_retries: int = 2


@dataclass
class OrchestrationContext:
    runtime: SessionRuntime
    registry: AgentRegistry
    llm: LiteLLMClient
    config: ExecutorConfig
    session_mgr: SessionManager
    executor: ChoirWorksAgentExecutor | None = None

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
