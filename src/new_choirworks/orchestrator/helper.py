from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import override

from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.base_node import BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.graph import DEFAULT_ROUTE
from new_choirworks.orchestrator.runtime import DagRuntime


def parse_mentions(text: str, agent_names: set[str]) -> list[tuple[str, str]]:
    """解析 ``@agent_name instruction`` 求助请求。

    语法约定（v0 契约，见 tests）：
    - ``@`` 后紧跟 agent 名（目录内），再紧跟一个空格，之后到下一个 ``@`` 之间
      是 instruction；``@reviewer``（无 instruction）、``@reviewer\\nxx``、
      ``@reviewer,xx``（名后非空格）都不命中；
    - 邮箱等含 ``@`` 的普通文本，只要 ``@`` 后首个 token 不是目录内 agent 名即不命中。

    返回 ``(agent_name, instruction)`` 列表。
    """
    results: list[tuple[str, str]] = []
    for part in text.split("@")[1:]:
        space_idx = part.find(" ")
        if space_idx == -1:
            continue
        agent_name = part[:space_idx]
        instruction = part[space_idx + 1 :].strip()
        if agent_name in agent_names and instruction:
            results.append((agent_name, instruction))
    return results


class Helper(BaseNode):
    """纯规则解析节点 output 中的 @mention（需求 8/9）。

    命中 → 加 assist 节点（接线结果交还），route="execute"；
    未命中 / 自引用 / 去重 / 超限 → DEFAULT_ROUTE → Planner 评估
    （replan 是默认动作，需求 14）。
    """

    runtime: DagRuntime

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        node_id = node_input if isinstance(node_input, str) else None
        if node_id is None or not self.runtime.in_plan(node_id):
            ctx.route = DEFAULT_ROUTE
            return
            yield  # pyright: ignore[reportUnreachable]

        output = self.runtime.node_outputs.get(node_id, "")
        parent = self.runtime.get_node(node_id)
        mentions = parse_mentions(output, set(self.runtime.agents))

        # 自引用（agent 求助自己）与同一 source+agent 重复求助忽略（防无限扩计划）
        candidates = [
            (agent_name, instruction)
            for agent_name, instruction in mentions
            if agent_name != parent.agent_name
            and not self.runtime.has_assist_for(node_id, agent_name)
        ]
        if not candidates:
            ctx.route = DEFAULT_ROUTE
            return
            yield  # pyright: ignore[reportUnreachable]

        # 深度/总量超限 → 不直接 append，交 Planner 决策（评审 P1-3）
        if not self.runtime.can_add_assists(node_id, len(candidates)):
            ctx.route = DEFAULT_ROUTE
            return
            yield  # pyright: ignore[reportUnreachable]

        for agent_name, instruction in candidates:
            _ = self.runtime.add_assist(node_id, agent_name, instruction)
        ctx.route = "execute"

        return
        yield  # pyright: ignore[reportUnreachable]
