from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from agent_hub.core.tasks import TaskNotFound
from agent_hub.models.enums import EventType, NodeStatus
from agent_hub.store import projections


class RollbackReport(BaseModel):
    checkpoint_id: str
    plan_version: int
    plan_id: str = ""
    reset_node_ids: list[str] = []
    invalidated_node_ids: list[str] = []
    cancelled_remote_task_ids: list[str] = []
    mode: str


def fold_graph(
    events: list[Any], seq: int, version: int
) -> tuple[str, dict[str, dict[str, Any]]] | None:
    plan_id: str | None = None
    nodes: dict[str, dict[str, Any]] = {}
    for event in events:
        if event.seq > seq:
            break
        payload = event.payload
        if event.type is EventType.PLAN_CREATED and payload.get("version") == version:
            plan_id = payload["plan_id"]
            nodes = {}
            for node in payload["dag"]["nodes"]:
                nodes[node["id"]] = {
                    "node": dict(node),
                    "deps": list(node.get("deps", [])),
                    "derived": bool(node.get("derived", False)),
                }
        elif (
            event.type is EventType.PLAN_EXTENDED
            and plan_id is not None
            and payload.get("plan_id") == plan_id
        ):
            for node in payload.get("added_nodes", []):
                nodes[node["id"]] = {
                    "node": dict(node),
                    "deps": list(node.get("deps", [])),
                    "derived": True,
                }
            for edge in payload.get("added_edges", []):
                target = nodes.get(edge["to"])
                if target is not None and edge["from"] not in target["deps"]:
                    target["deps"].append(edge["from"])
    if plan_id is None:
        return None
    return plan_id, nodes


async def _build_rollback(
    db: Any, events: Any, task_id: str, checkpoint_id: str
) -> tuple[RollbackReport, dict[str, Any]]:
    task = await projections.fetch_task(db, task_id)
    checkpoint = await projections.fetch_checkpoint(db, checkpoint_id)
    if task is None or checkpoint is None or checkpoint.task_id != task_id:
        raise TaskNotFound(f"task/checkpoint not found: {task_id}/{checkpoint_id}")
    replayed = await events.replay(task_id)
    folded = fold_graph(replayed, checkpoint.seq, checkpoint.plan_version)
    if folded is None:
        raise TaskNotFound(f"plan v{checkpoint.plan_version} not found for rollback")
    plan_id, graph = folded
    frontier = set(checkpoint.frontier)
    node_ids_at_checkpoint = {f"{plan_id}:{key}" for key in graph}

    deps_restore: dict[str, list[str]] = {}
    reset_node_ids: list[str] = []
    invalidate: list[str] = []
    for node in await projections.fetch_nodes(db, task_id):
        if node.id not in node_ids_at_checkpoint:
            if node.status is not NodeStatus.INVALIDATED:
                invalidate.append(node.id)
            continue
        key = node.id[len(plan_id) + 1 :]
        deps_restore[node.id] = [
            f"{plan_id}:{dep}" for dep in graph[key]["deps"]
        ]
        if node.id not in frontier:
            reset_node_ids.append(node.id)

    dag = {
        "nodes": [
            {**spec["node"], "deps": list(spec["deps"]), "derived": spec["derived"]}
            for spec in graph.values()
        ]
    }
    report = RollbackReport(
        checkpoint_id=checkpoint_id,
        plan_version=checkpoint.plan_version,
        plan_id=plan_id,
        reset_node_ids=reset_node_ids,
        invalidated_node_ids=invalidate,
        mode="dry_run",
    )
    payload = {
        "checkpoint_id": checkpoint_id,
        "plan_id": plan_id,
        "plan_version": checkpoint.plan_version,
        "dag": dag,
        "deps_restore": deps_restore,
        "reset_node_ids": reset_node_ids,
        "invalidate_node_ids": invalidate,
    }
    return report, payload


async def plan_rollback(
    db: Any, events: Any, task_id: str, checkpoint_id: str
) -> RollbackReport:
    report, _ = await _build_rollback(db, events, task_id, checkpoint_id)
    return report


async def perform_rollback(
    db: Any,
    events: Any,
    remote: Any,
    orchestrator: Any,
    task_id: str,
    checkpoint_id: str,
) -> RollbackReport:
    report, payload = await _build_rollback(db, events, task_id, checkpoint_id)
    cancel_ids = set(report.reset_node_ids) | set(report.invalidated_node_ids)
    cancellable = (
        NodeStatus.DISPATCHED,
        NodeStatus.WORKING,
        NodeStatus.INPUT_REQUIRED,
    )
    cancelled: list[str] = []
    for node in await projections.fetch_nodes(db, task_id):
        if node.id in cancel_ids and node.a2a_task_id and node.status in cancellable:
            await remote.cancel_task(node.agent_url or "", node.a2a_task_id)
            await events.append(
                task_id,
                EventType.NODE_CANCEL_SENT,
                {"node_id": node.id, "a2a_task_id": node.a2a_task_id},
            )
            cancelled.append(node.a2a_task_id)
    payload["cancelled_remote_task_ids"] = cancelled
    await events.append(task_id, EventType.ROLLBACK_PERFORMED, payload)
    orchestrator.start(task_id)
    return report.model_copy(
        update={"mode": "restart", "cancelled_remote_task_ids": cancelled}
    )
