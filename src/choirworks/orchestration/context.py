from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from a2a.server.events import EventQueue

from choirworks.a2a.room import RoomOptions
from choirworks.orchestration.session import SessionManager, SessionRuntime
from choirworks.orchestration.state import NodeState, OrchestrationState

if TYPE_CHECKING:
    from a2a.server.agent_execution import RequestContext
    from a2a.server.tasks.task_updater import TaskUpdater

    from choirworks.a2a.client import RemoteAgentClient
    from choirworks.core.llm import LiteLLMClient
    from choirworks.orchestration.hitl.intervention import QuestionResponse
    from choirworks.orchestration.prompts import ContextBriefBuilder
    from choirworks.orchestration.registry import AgentRegistry


@dataclass(frozen=True, slots=True)
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


@dataclass(slots=True, kw_only=True)
class OrchestrationContext:
    """One turn's orchestration context: TurnContext 协议的业务实现（无继承）。

    Extends the framework protocol with registry / state / sessions and the
    inbound parse (absorbed ``MessagePayload``).  ``task_id`` /
    ``context_id`` / ``queue`` / ``lock`` delegate to the session runtime so
    long-lived background runners always read the current values (a new
    inbound message updates runtime.task_id / runtime.queue).
    """

    # 会话运行时：跨轮活状态（state / queue / lock / runner / 任务表）
    runtime: SessionRuntime
    # agent 注册表（名称 → 记录）
    registry: AgentRegistry
    # 远端 peer 的 A2A 客户端
    remote: RemoteAgentClient
    # LLM 客户端（回合共享）
    llm: LiteLLMClient
    # 会话生命周期：load / ensure / persist / evict
    sessions: SessionManager
    # 执行配置：并行度 / 超时 / 重试 / 上限
    config: ExecutorConfig
    # 跨任务群聊历史摘要（超长时折叠）
    brief_builder: ContextBriefBuilder
    # —— 入站解析（吸收原 MessagePayload，由 _prepare_inbound / _classify_* 填充）
    # 本轮 A2A 请求；cancel 等无入站时为 None
    request: RequestContext | None = None
    # 本轮用户消息文本（strip 后）
    text: str = ""
    # 房间元数据（mentions / 引用 / 打断）
    room: RoomOptions = field(default_factory=RoomOptions)
    # 本轮 A2A 任务写入器（装配初始任务时创建）
    updater: TaskUpdater | None = None
    # 本轮解析出的问答响应
    responses: list[QuestionResponse] = field(default_factory=list)
    # 问答解析失败原因
    malformed: str | None = None
    # 分类标记：需要启动后台 runner
    needs_runner: bool = False
    # 分类结果：引用的节点 id
    quote_id: str | None = None
    # 分类结果：命中的目标节点
    target: NodeState | None = None

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
