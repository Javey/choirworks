from __future__ import annotations

import asyncio

from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.plan import DagPlan, PlanNode
from new_choirworks.orchestrator.runtime import (
    MAX_PLAN_NODES,
    DagRuntime,
    NodeStatus,
)

AGENTS = {"writer": AgentInfo(name="writer", url="http://writer")}


def _node(nid: str, deps: list[str] | None = None, agent: str = "writer") -> PlanNode:
    return PlanNode(id=nid, name=nid, agent_name=agent, instruction=f"task {nid}", deps=deps or [])


def _plan(*nodes: PlanNode) -> DagPlan:
    return DagPlan(nodes=list(nodes))


def _runtime(*nodes: PlanNode) -> DagRuntime:
    rt = DagRuntime(agents=AGENTS)
    rt.init_plan(_plan(*nodes))
    return rt


def test_init_plan_sets_pending():
    rt = _runtime(_node("a"), _node("b", deps=["a"]))
    assert rt.node_status["a"] == NodeStatus.PENDING
    assert rt.node_status["b"] == NodeStatus.PENDING


def test_dispatchable_respects_deps():
    rt = _runtime(_node("a"), _node("b", deps=["a"]))
    assert rt.dispatchable() == ["a"]
    rt.record_completion("a", "out_a")
    assert rt.dispatchable() == ["b"]


def test_dispatchable_skips_running_and_completed():
    rt = _runtime(_node("a"), _node("b"), _node("c"))
    rt.node_status["a"] = NodeStatus.RUNNING
    rt.record_completion("b", "out_b")
    assert rt.dispatchable() == ["c"]


def test_dispatchable_waiting_assist_when_deps_done():
    rt = _runtime(_node("a"))
    rt.record_completion("a", "need help")
    _ = rt.add_assist("a", "writer", "help a")
    assert set(rt.dispatchable()) == {rt.get_node("a").deps[0]}
    assist_id = rt.get_node("a").deps[0]
    rt.record_completion(assist_id, "helped")
    assert rt.dispatchable() == ["a"]


def test_has_work_variants():
    rt = _runtime(_node("a"))
    assert rt.has_work() is True
    rt.record_completion("a", "done")
    assert rt.has_work() is False

    empty = DagRuntime(agents=AGENTS)
    assert empty.has_work() is False


def test_apply_plan_keeps_completed_and_outputs():
    rt = _runtime(_node("a"), _node("b"))
    rt.record_completion("a", "out_a")
    rt.apply_plan(_plan(_node("a"), _node("c", deps=["a"])))

    assert rt.node_status["a"] == NodeStatus.COMPLETED
    assert rt.node_outputs["a"] == "out_a"
    assert rt.node_status["c"] == NodeStatus.PENDING
    # 移出计划不删状态历史（P1-1），但不可再派发
    assert "b" in rt.node_status
    assert rt.in_plan("b") is False


def test_apply_plan_running_not_cancelled():
    rt = _runtime(_node("a"), _node("b"))
    rt.node_status["b"] = NodeStatus.RUNNING
    rt.apply_plan(_plan(_node("a")))

    assert rt.node_status["b"] == NodeStatus.RUNNING
    assert "a" in rt.node_status


def test_apply_plan_new_nodes_pending():
    rt = _runtime(_node("a"))
    rt.record_completion("a", "out_a")
    rt.apply_plan(_plan(_node("a"), _node("b", deps=["a"])))
    assert rt.node_status["b"] == NodeStatus.PENDING


def test_apply_plan_waiting_assist_keeps_wiring():
    rt = _runtime(_node("a"))
    rt.record_completion("a", "help?")
    assist_id = rt.add_assist("a", "writer", "help a")
    rt.record_completion(assist_id, "helped")

    # 重规划：Planner 只回 a（deps 空）—— assist 接线必须保住（deps 并集 + output 可达）
    new_a = _node("a")
    rt.apply_plan(_plan(new_a))

    assert assist_id in new_a.deps
    assert rt.node_status["a"] == NodeStatus.WAITING_ASSIST
    assert rt.dispatchable() == ["a"]


def test_apply_plan_waiting_assist_drops_unreachable_dep():
    rt = _runtime(_node("a"))
    rt.record_completion("a", "help?")
    assist_id = rt.add_assist("a", "writer", "help a")

    # assist 未完成且被移出计划 → 依赖不可达，从 deps 剔除，a 继续推进
    new_a = _node("a")
    rt.apply_plan(_plan(new_a))

    assert assist_id not in new_a.deps
    assert rt.dispatchable() == ["a"]


def test_add_assist_wires_parent():
    rt = _runtime(_node("a"))
    rt.record_completion("a", "need help")
    assist_id = rt.add_assist("a", "writer", "help a")

    parent = rt.get_node("a")
    assert parent.deps == [assist_id]
    assert f"{{{{output:{assist_id}}}}}" in parent.instruction
    assert rt.node_status["a"] == NodeStatus.WAITING_ASSIST

    assist = rt.get_node(assist_id)
    assert assist.assist_for == "a"
    assert assist.deps == []
    assert "{{output:a}}" in assist.instruction
    assert rt.node_status[assist_id] == NodeStatus.PENDING


def test_completion_records():
    rt = _runtime(_node("a"))
    rt.record_completion("a", "out")
    rec = rt.pop_completion()
    assert rec is not None and rec.ok and rec.node_id == "a" and rec.output == "out"
    assert rt.pop_completion() is None

    rt.record_failure("a", "boom")
    assert rt.failed_completion() is not None


# --- 评审回归：状态历史 / ignored 完成 / 空计划 / assist 限流 ---


def test_apply_plan_keeps_status_history():
    """P1-1：移出计划不删状态历史，重新加入不重跑。"""
    rt = _runtime(_node("a"))
    rt.record_completion("a", "out_a")

    rt.apply_plan(_plan(_node("b")))
    assert rt.node_status["a"] == NodeStatus.COMPLETED
    assert rt.in_plan("a") is False

    rt.apply_plan(_plan(_node("a")))
    assert rt.node_status["a"] == NodeStatus.COMPLETED
    assert rt.dispatchable() == []


def test_removed_running_completion_not_judged():
    """P0-2：被移出计划的 running 节点完成后只留存结果，不触发判定。"""
    rt = _runtime(_node("a"))
    rt.node_status["a"] = NodeStatus.RUNNING
    rt.apply_plan(_plan(_node("b")))

    rt.record_completion("a", "@writer help")

    assert rt.node_outputs["a"] == "@writer help"
    assert rt.pop_completion() is None


def test_removed_failure_not_judged():
    """P1-2：被移出计划的节点失败只记录，不 abort 整体。"""
    rt = _runtime(_node("a"))
    rt.node_status["a"] = NodeStatus.RUNNING
    rt.apply_plan(_plan(_node("b")))

    rt.record_failure("a", "boom")

    assert rt.node_status["a"] == NodeStatus.FAILED
    assert rt.failed_completion() is None
    assert rt.pop_completion() is None


def test_empty_plan_stops_judgment():
    """P0-3：空计划 = 完成，之后的完成不再判定。"""
    rt = _runtime(_node("a"), _node("b"))
    rt.record_completion("a", "out_a")
    assert rt.pop_completion() is not None

    rt.record_completion("b", "out_b")
    rt.apply_plan(_plan())

    assert rt.done_requested is True
    assert rt.pop_completion() is None  # 队列被清空
    rt.record_completion("a", "again")
    assert rt.pop_completion() is None


def test_has_assist_for_dedupes():
    rt = _runtime(_node("a"))
    rt.record_completion("a", "help?")
    _ = rt.add_assist("a", "writer", "help a")
    assert rt.has_assist_for("a", "writer") is True
    assert rt.has_assist_for("a", "reviewer") is False


def test_assist_depth_limit():
    rt = _runtime(_node("a"))
    rt.record_completion("a", "help?")
    assert rt.can_add_assists("a", 1) is True
    aid1 = rt.add_assist("a", "writer", "help")
    assert rt.can_add_assists(aid1, 1) is True
    aid2 = rt.add_assist(aid1, "reviewer", "help")
    assert rt.can_add_assists(aid2, 1) is True
    aid3 = rt.add_assist(aid2, "writer", "help")
    # 深度 4 超过 MAX_ASSIST_DEPTH=3
    assert rt.can_add_assists(aid3, 1) is False


def test_assist_total_limit():
    rt = _runtime(_node("a"))
    rt.record_completion("a", "help?")
    for i in range(MAX_PLAN_NODES - 2):
        rt.plan.nodes.append(
            PlanNode(id=f"x{i}", name=f"x{i}", agent_name="writer", instruction="x")
        )
    assert rt.can_add_assists("a", 1) is True
    _ = rt.add_assist("a", "writer", "help")
    assert rt.can_add_assists("a", 1) is False


async def test_shutdown_cancels_running_tasks():
    rt = _runtime(_node("a"))
    started = asyncio.Event()
    cancelled: list[str] = []

    async def _slow() -> str:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.append("a")
            raise
        return "never"

    rt.node_status["a"] = NodeStatus.RUNNING
    rt.running_tasks["a"] = asyncio.create_task(_slow())
    _ = await started.wait()

    await rt.shutdown()

    assert cancelled == ["a"]
    assert rt.running_tasks == {}


def test_empty_plan_reset_by_non_empty_plan():
    """评审 P2-1：非空计划把 done_requested 重置回 False。"""
    rt = _runtime(_node("a"))
    rt.apply_plan(_plan())
    assert rt.done_requested is True

    rt.apply_plan(_plan(_node("a")))
    assert rt.done_requested is False


def test_apply_plan_preserves_assist_for():
    """评审 P2-2：Planner 重写计划后 assist_for 不丢，去重仍有效。"""
    rt = _runtime(_node("a"))
    rt.record_completion("a", "help?")
    assist_id = rt.add_assist("a", "reviewer", "help a")

    rewritten = PlanNode(
        id=assist_id,
        name="assist:reviewer",
        agent_name="reviewer",
        instruction="help a (rewritten)",
    )
    rt.apply_plan(_plan(rewritten))

    assert rewritten.assist_for == "a"
    assert rt.has_assist_for("a", "reviewer") is True
