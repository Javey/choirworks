# 设计参考 google-adk agents/base_agent.py（Apache-2.0, Copyright 2026 Google LLC）
from __future__ import annotations

import abc
from collections.abc import AsyncGenerator

from choirworks.core.agents.context import TurnContext
from choirworks.core.events import AgentEvent, ResultEvent


class BaseAgent(abc.ABC):
    """Agent 基类：身份 + agent 树 + 统一运行入口。

    ``run_async`` 是一个 ``AsyncGenerator[AgentEvent]``——yield A2A 标准事件
    供调用方推队列，最后 yield 一个 :class:`ResultEvent` 携带类型化结果。
    根 agent 的 ``ResultEvent`` 被丢弃（根返回 ``None``）；子 agent 的结果
    由调用方经 :func:`run_agent` 提取。

    子类可直接设类属性 ``name`` / ``description`` 作为默认值，无需在
    ``__init__`` 中传参；构造时传入同名关键字参数可覆盖类属性。
    """

    description: str = ""

    def __init__(
        self,
        *,
        name: str | None = None,
        description: str | None = None,
        sub_agents: list[BaseAgent] | None = None,
    ) -> None:
        if name is not None:
            self.name = name
        if not hasattr(self, "name"):
            raise TypeError(f"{type(self).__name__} requires a 'name'")
        if description is not None:
            self.description = description
        self.sub_agents: list[BaseAgent] = list(sub_agents or [])
        self.parent_agent: BaseAgent | None = None
        for sub_agent in self.sub_agents:
            if sub_agent.parent_agent is not None:
                raise ValueError(
                    f"agent `{sub_agent.name}` already has a parent agent"
                    f" `{sub_agent.parent_agent.name}`, trying to add: `{self.name}`"
                )
            sub_agent.parent_agent = self

    @abc.abstractmethod
    def run_async(
        self, ctx: TurnContext, user: str, **tool_kwargs: object
    ) -> AsyncGenerator[AgentEvent]:
        """跑一次调用：yield A2A 事件 + 最后 yield ResultEvent 携带结果。"""


async def run_agent[T](
    agent: BaseAgent,
    ctx: TurnContext,
    user: str,
    **kwargs: object,
) -> T | None:
    """跑 agent 并取回类型化结果：A2A 事件推 queue，ResultEvent 的值返回。"""
    async for event in agent.run_async(ctx, user, **kwargs):
        if isinstance(event, ResultEvent):
            return event.value  # pyright: ignore[reportReturnType]
        await ctx.queue.enqueue_event(event)
    return None
