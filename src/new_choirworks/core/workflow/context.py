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
    output: object | None = None
    route: RouteValue | None = None
    error: Exception | None = None
