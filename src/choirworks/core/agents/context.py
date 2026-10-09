# 设计参考 google-adk agents/invocation_context.py（Apache-2.0, Copyright 2026 Google LLC）：
# 仅借鉴「一次调用一个上下文、携带服务与队列」的概念；本仓无内部 Event，
# 事件经 queue 直推 A2A 标准事件（docs/agent-architecture-plan.md 决策 2）。
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from a2a.server.events import EventQueue

from choirworks.core.llm import LiteLLMClient


@dataclass(slots=True)
class TurnContext:
    """回合上下文基座（ADK InvocationContext 对应物）。

    业务子类扩展 registry / state / sessions / 入站解析；满足
    :class:`choirworks.core.events.EventSink`。
    """

    task_id: str
    context_id: str
    queue: EventQueue
    lock: asyncio.Lock
    llm: LiteLLMClient
