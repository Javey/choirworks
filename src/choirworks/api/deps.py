from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import Request

if TYPE_CHECKING:
    from a2a.server.tasks.task_store import TaskStore

    from choirworks.a2a.executor import ChoirWorksAgentExecutor
    from choirworks.orchestration.registry import AgentRegistry
    from choirworks.store.contexts import ContextStore


def get_task_store(request: Request) -> TaskStore:
    return request.app.state.task_store


def get_context_store(request: Request) -> ContextStore:
    return request.app.state.context_store


def get_registry(request: Request) -> AgentRegistry:
    return request.app.state.registry


def get_executor(request: Request) -> ChoirWorksAgentExecutor:
    return request.app.state.executor
