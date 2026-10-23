from __future__ import annotations

import asyncio
from collections import deque
from enum import Enum
from typing import final

from new_choirworks.core.workflow.errors import WorkflowError
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.plan import (
    DagPlan,
    PlanNode,
    output_ref,
    validate_plan,
)

MAX_ASSIST_DEPTH = 3
"""assist 链最大深度（A→B→C→D 拒绝），防 @mention 无限扩计划。"""

MAX_PLAN_NODES = 20
"""计划节点总数上限，与 validate_plan 默认值一致。"""


class NodeStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_ASSIST = "waiting_assist"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@final
class CompletionRecord:
    """一个子任务边界事件（节点级判定的输入）。"""

    def __init__(self, node_id: str, output: str = "", error: str | None = None) -> None:
        self.node_id = node_id
        self.output = output
        self.error = error

    @property
    def ok(self) -> bool:
        return self.error is None


@final
class DagRuntime:
    """一次编排运行的可变状态：计划、状态、output store、在途任务、边界队列。

    - ``node_outputs`` 是只增不删的 registry（结果留存，供 ``{{output:id}}`` 引用）；
    - ``completions`` 是待判定边界队列（消费优先，评估暂停语义）；
    - ``running_tasks`` 跨节点调用存活（已运行不取消）；
    - ``done_requested``：Planner 空计划 = 完成，之后只 drain 在途任务、不再判定。
    """

    def __init__(self, agents: dict[str, AgentInfo], user_task: str = "") -> None:
        self.agents = agents
        self.user_task = user_task
        self.plan = DagPlan()
        self.node_status: dict[str, NodeStatus] = {}
        self.node_outputs: dict[str, str] = {}
        self.running_tasks: dict[str, asyncio.Task[str]] = {}
        self.completions: deque[CompletionRecord] = deque()
        self.done_requested = False
        self._assist_seq = 0
        self._assist_depth: dict[str, int] = {}

    # ---- 计划 ----

    def init_plan(self, plan: DagPlan) -> None:
        validate_plan(plan)
        self.plan = plan
        for node in plan.nodes:
            self.node_status[node.id] = NodeStatus.PENDING

    def apply_plan(self, new_plan: DagPlan) -> None:
        """用 Planner 输出的完整新计划做按 id diff（需求 13）。

        - COMPLETED：幂等保留（不重跑；output 已在 registry）。
        - RUNNING：已运行不取消，按原输入跑完，结果留存。
        - PENDING / WAITING_ASSIST 且新计划保留：采用新规格；
          WAITING_ASSIST 的 deps 取并集（只保留仍可达的），保住 assist 接线，
          保证「B 的 output 交还给 A」不被重规划打断。
        - 旧有新无：不再派发（状态历史保留，重新加入不重跑）；RUNNING 继续跑完。
        - 新增：PENDING。
        - 空计划 = 完成（需求 15）：置 done_requested，此后只 drain、不再判定。
        """
        validate_plan(new_plan)
        if not new_plan.nodes:
            self.done_requested = True
            self.completions.clear()

        old = {n.id: n for n in self.plan.nodes}
        new = {n.id: n for n in new_plan.nodes}

        for node_id, node in new.items():
            status = self.node_status.get(node_id)
            if node_id not in old:
                if status is None or status in (NodeStatus.PENDING, NodeStatus.CANCELLED):
                    self.node_status[node_id] = NodeStatus.PENDING
                # RUNNING/WAITING_ASSIST/COMPLETED 状态保留：同 id 重加不重跑（幂等）
            elif status == NodeStatus.WAITING_ASSIST:
                merged: list[str] = []
                for dep in list(node.deps) + old[node_id].deps:
                    if dep not in merged and (dep in new or dep in self.node_outputs):
                        merged.append(dep)
                node.deps = merged

        self.plan = new_plan

    def get_node(self, node_id: str) -> PlanNode:
        for node in self.plan.nodes:
            if node.id == node_id:
                return node
        raise KeyError(f"node {node_id} not found in plan")

    def in_plan(self, node_id: str) -> bool:
        return any(node.id == node_id for node in self.plan.nodes)

    # ---- 调度 ----

    def dispatchable(self) -> list[str]:
        """可派发 = PENDING/WAITING_ASSIST 且所有 deps 的 output 已就绪。"""
        result: list[str] = []
        for node in self.plan.nodes:
            if self.node_status.get(node.id) not in (NodeStatus.PENDING, NodeStatus.WAITING_ASSIST):
                continue
            if all(dep in self.node_outputs for dep in node.deps):
                result.append(node.id)
        return result

    def has_work(self) -> bool:
        """「无 pending 无 running → Done」的操作化：还有在途任务或待派发/等协助节点。"""
        if self.running_tasks:
            return True
        return any(
            self.node_status.get(node.id) in (NodeStatus.PENDING, NodeStatus.WAITING_ASSIST)
            for node in self.plan.nodes
        )

    # ---- 边界事件 ----

    def _judgable(self, node_id: str) -> bool:
        """是否交给 Helper/Planner 判定：在当前计划内且未宣告完成。

        被移出计划的节点（含空计划后）只留存结果，不触发判定——避免
        ignored 完成被当成正常边界事件（评审 P0-2 / P0-3）。
        """
        return not self.done_requested and self.in_plan(node_id)

    def record_completion(self, node_id: str, output: str) -> None:
        self.node_status[node_id] = NodeStatus.COMPLETED
        self.node_outputs[node_id] = output
        if self._judgable(node_id):
            self.completions.append(CompletionRecord(node_id=node_id, output=output))

    def record_failure(self, node_id: str, error: str) -> None:
        self.node_status[node_id] = NodeStatus.FAILED
        if self._judgable(node_id):
            self.completions.append(CompletionRecord(node_id=node_id, error=error))

    def pop_completion(self) -> CompletionRecord | None:
        """弹出下一个可判定的边界事件（丢弃已被移出计划的过期记录）。"""
        while self.completions:
            rec = self.completions.popleft()
            if self._judgable(rec.node_id):
                return rec
        return None

    def failed_completion(self) -> CompletionRecord | None:
        return next(
            (rec for rec in self.completions if not rec.ok and self._judgable(rec.node_id)),
            None,
        )

    # ---- assist 生命周期（规则接线，Helper 调用） ----

    def has_assist_for(self, parent_id: str, agent_name: str) -> bool:
        """同一 source + agent 的 assist 是否已存在（去重，防重复扩计划）。"""
        return any(
            node.assist_for == parent_id and node.agent_name == agent_name
            for node in self.plan.nodes
        )

    def can_add_assists(self, parent_id: str, count: int) -> bool:
        """容量检查：assist 深度 ≤ MAX_ASSIST_DEPTH 且总数 ≤ MAX_PLAN_NODES。"""
        depth = self._assist_depth.get(parent_id, 0) + 1
        return depth <= MAX_ASSIST_DEPTH and len(self.plan.nodes) + count <= MAX_PLAN_NODES

    def add_assist(self, parent_id: str, agent_name: str, instruction: str) -> str:
        """@mention 命中：加 assist 节点并接线「结果交还给父节点」（需求 6 / 行 94）。

        - assist 节点 deps=[]（立即可派发），instruction 附父节点 output 引用；
        - 父节点 deps += assist、instruction 附 ``{{output:assist}}`` 引用，
          标 WAITING_ASSIST（不标记完成，等待续轮）。
        """
        self._assist_seq += 1
        assist_id = f"{parent_id}-assist{self._assist_seq}"
        parent = self.get_node(parent_id)

        assist = PlanNode(
            id=assist_id,
            name=f"assist:{agent_name}",
            agent_name=agent_name,
            instruction=f"{instruction}\n\n[{parent_id} output]\n{output_ref(parent_id)}",
            deps=[],
            assist_for=parent_id,
        )
        self.plan.nodes.append(assist)
        self.node_status[assist_id] = NodeStatus.PENDING
        self._assist_depth[assist_id] = self._assist_depth.get(parent_id, 0) + 1

        if assist_id not in parent.deps:
            parent.deps.append(assist_id)
        parent.instruction += f"\n\n[{assist_id} output]\n{output_ref(assist_id)}"
        self.node_status[parent_id] = NodeStatus.WAITING_ASSIST
        return assist_id

    # ---- 生命周期 ----

    async def shutdown(self) -> None:
        """取消并收拢全部本地在途任务（幂等；整体失败/异常/取消统一走这里）。

        TODO(cancel): 应向在途远端任务发 CancelTask（a2a Client.cancel_task）并停
        本地流消费，待 RemoteA2aAgent 支持 cancel 后接入；当前仅取消本地 asyncio 任务。
        """
        tasks = list(self.running_tasks.values())
        self.running_tasks.clear()
        for task in tasks:
            if not task.done():
                _ = task.cancel()
        if tasks:
            _ = await asyncio.gather(*tasks, return_exceptions=True)

    async def abort(self, cause: str) -> None:
        """FAILED → 整体失败：清理后抛 WorkflowError。"""
        await self.shutdown()
        raise WorkflowError(cause)
