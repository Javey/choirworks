# 设计参考 google-adk agents/base_agent.py（Apache-2.0, Copyright 2026 Google LLC）
from __future__ import annotations

import abc
from dataclasses import dataclass, field

from a2a.server.agent_execution import RequestContext
from a2a.server.events import EventQueue


@dataclass(slots=True)
class BaseAgent(abc.ABC):
    """Agent 基类：身份 + agent 树 + 回合运行入口。

    ``run_async`` 是根 agent 的回合入口：一条入站消息进，事件经
    ``event_queue`` 出。对应 ADK ``run_async`` 的事件流，本仓事件直接推
    队列，不走 Event 生成器。
    """

    name: str
    description: str = ""
    sub_agents: list[BaseAgent] = field(default_factory=list)
    parent_agent: BaseAgent | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        for sub_agent in self.sub_agents:
            if sub_agent.parent_agent is not None:
                raise ValueError(
                    f"agent `{sub_agent.name}` already has a parent agent"
                    f" `{sub_agent.parent_agent.name}`, trying to add: `{self.name}`"
                )
            sub_agent.parent_agent = self

    @abc.abstractmethod
    async def run_async(self, context: RequestContext, event_queue: EventQueue) -> None:
        """处理一条入站消息，事件经 ``event_queue`` 发布。"""
