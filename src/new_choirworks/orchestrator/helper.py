from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import override

from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.base_node import BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.graph import DEFAULT_ROUTE
from new_choirworks.orchestrator.runtime import DagRuntime


def parse_mentions(text: str, agent_names: set[str]) -> list[tuple[str, str]]:
    """解析 ``@agent_name instruction``（instruction 到下一个 @ 为止）。

    只认 agent 目录内的名字；无 instruction 的 @ 忽略。
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
    未命中 → DEFAULT_ROUTE → Planner 评估（replan 是默认动作，需求 14）。
    """

    runtime: DagRuntime

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        node_id = node_input if isinstance(node_input, str) else None
        if node_id is None:
            ctx.route = DEFAULT_ROUTE
            return
            yield  # pyright: ignore[reportUnreachable]

        output = self.runtime.node_outputs.get(node_id, "")
        mentions = parse_mentions(output, set(self.runtime.agents))
        if not mentions:
            ctx.route = DEFAULT_ROUTE
            return
            yield  # pyright: ignore[reportUnreachable]

        for agent_name, instruction in mentions:
            _ = self.runtime.add_assist(node_id, agent_name, instruction)
        ctx.route = "execute"

        return
        yield  # pyright: ignore[reportUnreachable]
