# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/agents/context.py
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from new_choirworks.core.agents.invocation_context import InvocationContext

if TYPE_CHECKING:
    from new_choirworks.core.workflow.base_node import BaseNode, RouteValue


@dataclass
class Context:
    """Per-node execution context.

    Carries per-node state (output, route, error) while sharing
    InvocationContext across all nodes in a workflow run.
    """

    invocation_context: InvocationContext
    node: BaseNode
    # TODO: 迁移到 event.output / event.actions.route（EventMeta）。
    # 收益：① ADK 同构——_track_event_in_context promote 模式（event 为源，
    # ctx 为派生）；② RemoteA2aAgent 产出的远端 event 自带 output，NodeRunner
    # 统一 promote 到 ctx，RemoteA2aAgent 不需耦合 Context；③ event.metadata
    # 随 A2A SDK 自动持久化，为 resume/replay 打基础。
    output: object | None = None
    route: RouteValue | None = None
    error: Exception | None = None
