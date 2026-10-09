# 设计参考 google-adk agents/base_agent.py（Apache-2.0, Copyright 2026 Google LLC）
from __future__ import annotations

import abc

from choirworks.core.agents.context import TurnContext


class BaseAgent(abc.ABC):
    """Agent 基类：身份 + agent 树 + 统一运行入口。

    ``run_async`` 对应 ADK ``run_async``：吃一条用户消息、产出输出——根
    agent 返回 ``None``（回合驱动），LlmAgent 返回类型化结果；事件经
    ``ctx.queue`` 直推 A2A 标准事件，不走 Event 生成器（决策 2）。返回
    ``object`` 是砍掉内部 Event 后对 ADK ``AsyncGenerator[Event]`` 的替代。

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
    async def run_async(self, ctx: TurnContext, user: str, **tool_kwargs: object) -> object:
        """跑一次调用：吃一条用户消息，产出输出；事件经 ctx 推出。"""
