# 设计参考 google-adk runners.py（Apache-2.0, Copyright 2026 Google LLC）：
# 仅借鉴「一个 Runner 驱动根 agent、会话装配与执行分离」的结构；本仓无
# 内部 Event，agent yield A2A 事件经 Runner 推队列（决策 2）。
from __future__ import annotations

from collections.abc import Awaitable, Callable

import structlog
from a2a.server.agent_execution import RequestContext
from a2a.server.events import EventQueue

from choirworks.core.agents.base import BaseAgent
from choirworks.core.agents.context import TurnContext
from choirworks.core.events import ResultEvent

logger = structlog.get_logger(__name__)


class Runner:
    """回合驱动：装配回合上下文、持锁驱动根 agent（ADK Runner 对应物）。

    业务侧把服务与会话装配封在 *prepare* 闭包里（ensure_session + 业务
    回合子类），core 只认识「怎么装配上下文」与「根 agent」。
    """

    def __init__(
        self,
        root: BaseAgent,
        prepare: Callable[[RequestContext, EventQueue], Awaitable[TurnContext]],
    ) -> None:
        self._root = root
        self._prepare = prepare

    async def run_async(self, context: RequestContext, event_queue: EventQueue) -> None:
        """处理一条入站消息：装配上下文，持锁驱动根 agent 跑一个回合。"""
        ctx = await self._prepare(context, event_queue)
        user = (context.get_user_input() or "").strip()
        logger.info(
            "runner",
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            agent=self._root.name,
        )
        async with ctx.lock:
            async for event in self._root.run_async(ctx, user):
                if not isinstance(event, ResultEvent):
                    await event_queue.enqueue_event(event)
