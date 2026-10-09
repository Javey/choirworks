# 设计参考 google-adk agents/invocation_context.py（Apache-2.0, Copyright 2026 Google LLC）：
# 仅借鉴「一次调用一个上下文、携带服务与队列」的概念；本仓无内部 Event，
# 事件经 queue 直推 A2A 标准事件（docs/agent-architecture-plan.md 决策 2）。
from __future__ import annotations

import asyncio
from typing import Protocol

from choirworks.core.events import EventSink
from choirworks.core.llm import LiteLLMClient


class TurnContext(EventSink, Protocol):
    """回合上下文协议（ADK InvocationContext 对应物）。

    core 只声明「agent 需要什么形状的 ctx」：身份 / 队列（``EventSink``）
    + 锁 + LLM 客户端。业务侧以普通类实现（``orchestration.context.
    OrchestrationContext``），无需继承。
    """

    @property
    def lock(self) -> asyncio.Lock: ...

    @property
    def llm(self) -> LiteLLMClient: ...
