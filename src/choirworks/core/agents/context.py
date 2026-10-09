# 设计参考 google-adk agents/invocation_context.py（Apache-2.0, Copyright 2026 Google LLC）：
# 仅借鉴「一次调用一个上下文、携带服务与队列」的概念；本仓无内部 Event，
# 事件经 queue 直推 A2A 标准事件（docs/agent-architecture-plan.md 决策 2）。
from __future__ import annotations

import asyncio

from a2a.server.events import EventQueue

from choirworks.core.llm import LiteLLMClient


class TurnContext:
    """回合上下文基座（ADK InvocationContext 对应物）。

    业务子类（``orchestration.context.OrchestrationContext``）扩展
    registry / state / sessions / 入站解析；满足
    :class:`choirworks.core.events.EventSink`。

    字段以私有属性存储、公开为只读 property——业务子类可覆写为动态委托
    （如从会话运行时读取，见 ``OrchestrationContext``）。
    """

    def __init__(
        self,
        task_id: str,
        context_id: str,
        queue: EventQueue,
        lock: asyncio.Lock,
        llm: LiteLLMClient,
    ) -> None:
        self._task_id = task_id
        self._context_id = context_id
        self._queue = queue
        self._lock = lock
        self._llm = llm

    @property
    def task_id(self) -> str:
        return self._task_id

    @property
    def context_id(self) -> str:
        return self._context_id

    @property
    def queue(self) -> EventQueue:
        return self._queue

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    @property
    def llm(self) -> LiteLLMClient:
        return self._llm
