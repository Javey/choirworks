# DAG 计划数据结构。结构校验同构 legacy choirworks/core/planner.py validate_plan。
from __future__ import annotations

import re

from pydantic import BaseModel, Field

REF_RE = re.compile(r"\{\{output:([A-Za-z0-9_.\-]+)\}\}")


class PlanNode(BaseModel):
    """DAG 中的一个子任务节点。

    ``id`` 为唯一标识（deps 引用与 output 引用都用它）；``name`` 是展示名。
    ``instruction`` 支持 ``{{output:<node_id>}}`` 占位符，派发时从 outputs store 解析。
    """

    id: str
    name: str
    agent_name: str
    instruction: str
    deps: list[str] = Field(default_factory=list)
    assist_for: str | None = None


class DagPlan(BaseModel):
    """Planner 输出的完整 DAG 计划（空计划 = 完成）。"""

    nodes: list[PlanNode] = Field(default_factory=list)


class PlanValidationError(ValueError):
    """DagPlan 结构校验失败。"""


def output_ref(node_id: str) -> str:
    """构造 instruction 里的 output 引用占位符。"""
    return "{{output:" + node_id + "}}"


def resolve_instruction(instruction: str, outputs: dict[str, str]) -> str:
    """把 ``{{output:<id>}}`` 占位符替换为已有 output；缺失的解析为空串。"""
    return REF_RE.sub(lambda m: outputs.get(m.group(1), ""), instruction)


def validate_plan(plan: DagPlan, max_nodes: int = 20) -> None:
    """结构校验：节点数上限 / id 唯一 / 依赖存在 / 无自依赖 / 无环（Kahn）。"""
    if len(plan.nodes) > max_nodes:
        raise PlanValidationError(f"too many nodes: {len(plan.nodes)} > {max_nodes}")

    ids = [node.id for node in plan.nodes]
    if len(set(ids)) != len(ids):
        raise PlanValidationError("duplicate node id in plan")

    id_set = set(ids)
    for node in plan.nodes:
        for dep in node.deps:
            if dep not in id_set:
                raise PlanValidationError(f"unknown dependency: {dep}")
            if dep == node.id:
                raise PlanValidationError(f"node {node.id} depends on itself")

    indegree: dict[str, int] = {node.id: len(set(node.deps)) for node in plan.nodes}
    children: dict[str, list[str]] = {node.id: [] for node in plan.nodes}
    for node in plan.nodes:
        for dep in set(node.deps):
            children[dep].append(node.id)

    queue = [node_id for node_id, degree in indegree.items() if degree == 0]
    visited = 0
    while queue:
        current = queue.pop()
        visited += 1
        for child in children[current]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if visited != len(ids):
        raise PlanValidationError("plan contains a cycle")
