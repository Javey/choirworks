from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from typing import Literal, override

from pydantic import create_model

from new_choirworks.core.agents.llm_agent import LLMAgent
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.models.base_llm import BaseLLM
from new_choirworks.core.workflow.base_node import BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.llm_agent_wrapper import run_llm_agent_as_node
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.plan import DagPlan, PlanNode
from new_choirworks.orchestrator.runtime import DagRuntime


# Enum-pinned structured output mirrors google-adk's TransferToAgentTool
# (src/google/adk/tools/transfer_to_agent_tool.py, Apache-2.0, Copyright 2026 Google LLC),
# which constrains agent_name to a JSON-Schema enum so hallucinated names cannot
# pass validation.
def constrained_plan_schema(agent_names: Sequence[str]) -> type[DagPlan]:
    """动态构造 DagPlan 子类，把 agent_name 约束到注册目录的 Literal 枚举。"""
    if not agent_names:
        raise ValueError("agent catalog is empty: at least one agent is required")
    constrained_node = create_model(
        "PlanNode",
        __base__=PlanNode,
        agent_name=(Literal[*agent_names], ...),  # type: ignore[valid-type]
    )
    return create_model(
        "DagPlan",
        __base__=DagPlan,
        nodes=(list[constrained_node], ...),  # type: ignore[valid-type]
    )


def build_planner_instruction(agents: Sequence[AgentInfo]) -> str:
    """Planner 系统指令：唯一 LLM 决策点，输出完整新计划（需求 13/14/15）。"""
    capability_lines = "\n".join(f"- {agent.name}: {agent.description}" for agent in agents)
    return (
        "You are the planning brain of a multi-agent orchestration platform.\n"
        "You are the SOLE decision point: after every node completion you evaluate the\n"
        "full context and output the complete new plan (never a diff / change list).\n"
        "\n"
        "Available agents:\n"
        f"{capability_lines}\n"
        "\n"
        "Rules:\n"
        "- agent_name is enum-constrained to the registered agents listed above.\n"
        "- Use deps to express ordering; independent nodes run in parallel.\n"
        "- Each instruction MUST be self-contained: restate everything the agent needs\n"
        "  (including its slice of the user goal); agents have no conversation memory.\n"
        "- Reference another node's output in an instruction as {{output:<node_id>}}.\n"
        "- Keep completed nodes (same id) in the plan when their output is still needed;\n"
        "  completed nodes are never re-run, their output stays available.\n"
        "- Keep waiting nodes and their deps intact: a node waiting for assist results\n"
        "  must continue (its next round receives the assist output).\n"
        "- Do not invent node ids that already finished under a different meaning;\n"
        "  reuse the same id only to keep the same task.\n"
        "- Empty nodes list means the whole job is complete: nothing more to run.\n"
    )


def build_planner(agents: Sequence[AgentInfo], llm: BaseLLM) -> LLMAgent:
    """构造 Planner LLMAgent（output_schema 约束到注册 agent 目录）。"""
    return LLMAgent(
        name="planner",
        llm=llm,
        instruction=build_planner_instruction(agents),
        output_schema=constrained_plan_schema([a.name for a in agents]),
        include_contents="none",
    )


def build_planner_brief(runtime: DagRuntime) -> str:
    """给 Planner 的确定性全量上下文：任务 + 当前计划状态 + outputs registry。"""
    lines: list[str] = [f"## User task\n{runtime.user_task}", "\n## Current plan"]
    for node in runtime.plan.nodes:
        status = runtime.node_status.get(node.id)
        deps = ",".join(node.deps) if node.deps else "-"
        state = status.value if status else "?"
        assist = f", assist_for={node.assist_for}" if node.assist_for else ""
        header = f"- {node.id} (agent={node.agent_name}, status={state}, deps={deps}{assist})"
        lines.append(f"{header}\n  instruction: {node.instruction}")
    if not runtime.plan.nodes:
        lines.append("(empty)")
    lines.append("\n## Node outputs")
    if runtime.node_outputs:
        for node_id, output in runtime.node_outputs.items():
            lines.append(f"[{node_id} output]\n{output}")
    else:
        lines.append("(none)")
    return "\n".join(lines)


class PlannerNode(BaseNode):
    """Planner 节点：读全量上下文 brief，输出完整新计划（空计划 = 完成）。"""

    planner: LLMAgent
    runtime: DagRuntime

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        async for event in run_llm_agent_as_node(
            self.planner, ctx=ctx, node_input=build_planner_brief(self.runtime)
        ):
            yield event
