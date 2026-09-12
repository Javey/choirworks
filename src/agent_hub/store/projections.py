from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import aiosqlite

from agent_hub.models.domain import Node, OrchestrationTask, Plan
from agent_hub.models.enums import (
    TERMINAL_NODE_STATUSES,
    EventType,
    NodeStatus,
    TaskStatus,
)


async def apply_event(conn: aiosqlite.Connection, event: Any) -> None:
    payload = event.payload
    ts = event.created_at.isoformat()
    event_type = event.type

    if event_type is EventType.TASK_CREATED:
        await conn.execute(
            "INSERT INTO orchestration_tasks"
            " (id, status, request, policy, plan_version, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                event.task_id,
                TaskStatus.PLANNING.value,
                payload["request"],
                json.dumps(payload.get("policy"), ensure_ascii=False),
                None,
                ts,
                ts,
            ),
        )
    elif event_type is EventType.PLAN_CREATED:
        await conn.execute(
            "INSERT INTO plans (id, task_id, version, dag, rationale, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                payload["plan_id"],
                event.task_id,
                payload["version"],
                json.dumps(payload["dag"], ensure_ascii=False),
                payload.get("rationale"),
                ts,
            ),
        )
        await conn.execute(
            "UPDATE orchestration_tasks SET plan_version = ?, updated_at = ? WHERE id = ?",
            (payload["version"], ts, event.task_id),
        )
        await _materialize_nodes(conn, event.task_id, payload["plan_id"], payload["dag"])
    elif event_type is EventType.TASK_STATE_CHANGED:
        await conn.execute(
            "UPDATE orchestration_tasks SET status = ?, updated_at = ? WHERE id = ?",
            (payload["to"], ts, event.task_id),
        )
    elif event_type in (EventType.TASK_COMPLETED, EventType.TASK_FAILED):
        status = (
            TaskStatus.COMPLETED.value
            if event_type is EventType.TASK_COMPLETED
            else TaskStatus.FAILED.value
        )
        await conn.execute(
            "UPDATE orchestration_tasks SET status = ?, updated_at = ? WHERE id = ?",
            (status, ts, event.task_id),
        )
    elif event_type is EventType.NODE_DISPATCH_INTENT:
        await conn.execute(
            "UPDATE nodes SET attempt = ?, started_at = COALESCE(started_at, ?)"
            " WHERE id = ? AND task_id = ?",
            (payload["attempt"], ts, payload["node_id"], event.task_id),
        )
    elif event_type is EventType.NODE_DISPATCHED:
        await conn.execute(
            "UPDATE nodes SET status = ?, a2a_task_id = ?, a2a_context_id = ?"
            " WHERE id = ? AND task_id = ?",
            (
                NodeStatus.DISPATCHED.value,
                payload["a2a_task_id"],
                payload.get("a2a_context_id"),
                payload["node_id"],
                event.task_id,
            ),
        )
    elif event_type is EventType.NODE_STATE_CHANGED:
        target = NodeStatus(payload["to"])
        ended_at = ts if target in TERMINAL_NODE_STATUSES else None
        await conn.execute(
            "UPDATE nodes SET status = ?, ended_at = COALESCE(?, ended_at)"
            " WHERE id = ? AND task_id = ?",
            (target.value, ended_at, payload["node_id"], event.task_id),
        )
    elif event_type is EventType.NODE_OUTPUT:
        await conn.execute(
            "UPDATE nodes SET output = ?, ended_at = COALESCE(ended_at, ?)"
            " WHERE id = ? AND task_id = ?",
            (
                json.dumps(payload["output"], ensure_ascii=False),
                ts,
                payload["node_id"],
                event.task_id,
            ),
        )
    elif event_type is EventType.ERROR and payload.get("node_id"):
        await conn.execute(
            "UPDATE nodes SET error = ? WHERE id = ? AND task_id = ?",
            (payload.get("message"), payload["node_id"], event.task_id),
        )


async def _materialize_nodes(
    conn: aiosqlite.Connection,
    task_id: str,
    plan_id: str,
    dag: dict[str, Any],
) -> None:
    for node in dag["nodes"]:
        node_id = f"{plan_id}:{node['id']}"
        deps = [f"{plan_id}:{dep}" for dep in node.get("deps", [])]
        await conn.execute(
            "INSERT INTO nodes"
            " (id, task_id, plan_id, name, agent_url, skill_id, deps, input,"
            "  status, attempt, requires_approval)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
            (
                node_id,
                task_id,
                plan_id,
                node["name"],
                node.get("agent_url"),
                node.get("skill_id"),
                json.dumps(deps),
                json.dumps(node.get("input"), ensure_ascii=False),
                NodeStatus.PENDING.value,
                1 if node.get("requires_approval") else 0,
            ),
        )


async def rebuild(db: Any) -> None:
    from agent_hub.store.event_store import EventStore

    store = EventStore(db)
    async with db.transaction() as conn:
        for table in ("nodes", "plans", "orchestration_tasks", "interventions", "checkpoints"):
            await conn.execute(f"DELETE FROM {table}")
    events = await store.replay_all()
    for event in events:
        async with db.transaction() as conn:
            await apply_event(conn, event)


def _parse_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _row_to_task(row: aiosqlite.Row) -> OrchestrationTask:
    return OrchestrationTask(
        id=row["id"],
        status=TaskStatus(row["status"]),
        request=row["request"],
        policy=json.loads(row["policy"]) if row["policy"] else None,
        plan_version=row["plan_version"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


def _row_to_plan(row: aiosqlite.Row) -> Plan:
    return Plan(
        id=row["id"],
        task_id=row["task_id"],
        version=row["version"],
        dag=json.loads(row["dag"]),
        rationale=row["rationale"],
        created_at=datetime.fromisoformat(row["created_at"]),
    )


def _row_to_node(row: aiosqlite.Row) -> Node:
    return Node(
        id=row["id"],
        task_id=row["task_id"],
        plan_id=row["plan_id"],
        name=row["name"],
        agent_url=row["agent_url"],
        skill_id=row["skill_id"],
        deps=json.loads(row["deps"]),
        input=json.loads(row["input"]) if row["input"] else None,
        status=NodeStatus(row["status"]),
        attempt=row["attempt"],
        requires_approval=bool(row["requires_approval"]),
        a2a_task_id=row["a2a_task_id"],
        a2a_context_id=row["a2a_context_id"],
        output=json.loads(row["output"]) if row["output"] else None,
        error=row["error"],
        started_at=_parse_dt(row["started_at"]),
        ended_at=_parse_dt(row["ended_at"]),
    )


async def fetch_task(db: Any, task_id: str) -> OrchestrationTask | None:
    cursor = await db.conn.execute(
        "SELECT * FROM orchestration_tasks WHERE id = ?", (task_id,)
    )
    row = await cursor.fetchone()
    return _row_to_task(row) if row else None


async def fetch_current_plan(db: Any, task_id: str) -> Plan | None:
    cursor = await db.conn.execute(
        "SELECT p.* FROM plans p JOIN orchestration_tasks t"
        " ON p.task_id = t.id AND p.version = t.plan_version"
        " WHERE t.id = ?",
        (task_id,),
    )
    row = await cursor.fetchone()
    return _row_to_plan(row) if row else None


async def fetch_node(db: Any, node_id: str) -> Node | None:
    cursor = await db.conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,))
    row = await cursor.fetchone()
    return _row_to_node(row) if row else None


async def fetch_nodes(db: Any, task_id: str, plan_id: str | None = None) -> list[Node]:
    if plan_id is None:
        cursor = await db.conn.execute(
            "SELECT * FROM nodes WHERE task_id = ? ORDER BY id", (task_id,)
        )
    else:
        cursor = await db.conn.execute(
            "SELECT * FROM nodes WHERE task_id = ? AND plan_id = ? ORDER BY id",
            (task_id, plan_id),
        )
    return [_row_to_node(row) for row in await cursor.fetchall()]
