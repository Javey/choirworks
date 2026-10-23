from __future__ import annotations

from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.plan import DagPlan, PlanNode
from new_choirworks.orchestrator.runtime import DagRuntime, NodeStatus

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
    assert "b" not in rt.node_status


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
