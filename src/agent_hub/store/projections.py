from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import aiosqlite

from agent_hub.models.domain import (
    Checkpoint,
    Conversation,
    ConversationSummary,
    Intervention,
    Node,
    OrchestrationTask,
    Plan,
    RoomMember,
    RoomMessage,
    RoomSummary,
)
from agent_hub.models.enums import (
    TERMINAL_NODE_STATUSES,
    EventType,
    InterventionStatus,
    NodeStatus,
    TaskStatus,
)


async def apply_event(conn: aiosqlite.Connection, event: Any) -> None:
    payload = event.payload
    ts = event.created_at.isoformat()
    event_type = event.type

    if event_type is EventType.TASK_CREATED:
        conversation_id = payload.get("conversation_id")
        if conversation_id:
            await conn.execute(
                "INSERT INTO conversations (id, title, created_at) VALUES (?, ?, ?)"
                " ON CONFLICT(id) DO NOTHING",
                (
                    conversation_id,
                    payload.get("conversation_title") or payload["request"][:60],
                    ts,
                ),
            )
        await conn.execute(
            "INSERT INTO orchestration_tasks"
            " (id, status, request, policy, plan_version, conversation_id, created_at,"
            "  updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event.task_id,
                TaskStatus.PLANNING.value,
                payload["request"],
                json.dumps(payload.get("policy"), ensure_ascii=False),
                None,
                conversation_id,
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
    elif event_type is EventType.PLAN_EXTENDED:
        plan_id = payload["plan_id"]
        added_nodes = payload.get("added_nodes", [])
        added_edges = payload.get("added_edges", [])
        for node in added_nodes:
            await _materialize_nodes(conn, event.task_id, plan_id, {"nodes": [node]})
        for edge in added_edges:
            from_id = f"{plan_id}:{edge['from']}"
            to_id = f"{plan_id}:{edge['to']}"
            cursor = await conn.execute("SELECT deps FROM nodes WHERE id = ?", (to_id,))
            row = await cursor.fetchone()
            if row is not None:
                deps = json.loads(row["deps"])
                if from_id not in deps:
                    deps.append(from_id)
                    await conn.execute(
                        "UPDATE nodes SET deps = ? WHERE id = ?",
                        (json.dumps(deps), to_id),
                    )
        cursor = await conn.execute("SELECT dag FROM plans WHERE id = ?", (plan_id,))
        row = await cursor.fetchone()
        if row is not None:
            dag = json.loads(row["dag"])
            for node in added_nodes:
                dag["nodes"].append(node)
            for edge in added_edges:
                for node in dag["nodes"]:
                    if node["id"] == edge["to"]:
                        deps = node.setdefault("deps", [])
                        if edge["from"] not in deps:
                            deps.append(edge["from"])
            await conn.execute(
                "UPDATE plans SET dag = ? WHERE id = ?",
                (json.dumps(dag, ensure_ascii=False), plan_id),
            )
    elif event_type is EventType.PLAN_SUPERSEDED:
        await conn.execute(
            "UPDATE interventions SET status = ? WHERE task_id = ? AND status = ?",
            (
                InterventionStatus.INVALIDATED.value,
                event.task_id,
                InterventionStatus.PENDING.value,
            ),
        )
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
    elif event_type is EventType.INTERVENTION_REQUESTED:
        await conn.execute(
            "INSERT INTO interventions"
            " (id, task_id, node_id, assigned_node_id, assigned_to, source, policy,"
            "  question, responder, status, deadline_at, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                payload["intervention_id"],
                event.task_id,
                payload.get("node_id"),
                payload.get("assigned_node_id"),
                payload.get("assigned_to"),
                payload["source"],
                payload["policy"],
                json.dumps(payload.get("question"), ensure_ascii=False),
                payload.get("responder"),
                InterventionStatus.PENDING.value,
                payload.get("deadline_at"),
                ts,
            ),
        )
    elif event_type is EventType.INTERVENTION_FAILED:
        await conn.execute(
            "UPDATE interventions SET status = ? WHERE id = ?",
            (InterventionStatus.FAILED.value, payload["intervention_id"]),
        )
    elif event_type is EventType.INTERVENTION_RESOLVED:
        await conn.execute(
            "UPDATE interventions SET status = ?, answer = ?, responder = ?,"
            " resolved_at = ? WHERE id = ?",
            (
                InterventionStatus.RESOLVED.value,
                json.dumps(payload.get("answer"), ensure_ascii=False),
                payload.get("responder"),
                ts,
                payload["intervention_id"],
            ),
        )
    elif event_type is EventType.CHECKPOINT_CREATED:
        await conn.execute(
            "INSERT INTO checkpoints"
            " (id, task_id, seq, plan_version, frontier, artifacts, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                payload["checkpoint_id"],
                event.task_id,
                payload["seq"],
                payload["plan_version"],
                json.dumps(payload["frontier"]),
                json.dumps(payload["artifacts"], ensure_ascii=False),
                ts,
            ),
        )
    elif event_type is EventType.ROLLBACK_PERFORMED:
        await conn.execute(
            "UPDATE orchestration_tasks SET status = ?, plan_version = ?, updated_at = ?"
            " WHERE id = ?",
            (
                TaskStatus.RUNNING.value,
                payload["plan_version"],
                ts,
                event.task_id,
            ),
        )
        for node_id, deps in (payload.get("deps_restore") or {}).items():
            await conn.execute(
                "UPDATE nodes SET deps = ? WHERE id = ? AND task_id = ?",
                (json.dumps(deps), node_id, event.task_id),
            )
        for node_id in payload.get("reset_node_ids", []):
            await conn.execute(
                "UPDATE nodes SET status = 'pending', attempt = 0, output = NULL,"
                " error = NULL, a2a_task_id = NULL, a2a_context_id = NULL,"
                " started_at = NULL, ended_at = NULL WHERE id = ? AND task_id = ?",
                (node_id, event.task_id),
            )
        for node_id in payload.get("invalidate_node_ids", []):
            await conn.execute(
                "UPDATE nodes SET status = ?, a2a_task_id = NULL,"
                " a2a_context_id = NULL WHERE id = ? AND task_id = ?",
                (
                    NodeStatus.INVALIDATED.value,
                    node_id,
                    event.task_id,
                ),
            )
        if payload.get("plan_id") and payload.get("dag"):
            await conn.execute(
                "UPDATE plans SET dag = ? WHERE id = ?",
                (json.dumps(payload["dag"], ensure_ascii=False), payload["plan_id"]),
            )
        affected_nodes = payload.get("reset_node_ids", []) + payload.get(
            "invalidate_node_ids", []
        )
        for node_id in affected_nodes:
            await conn.execute(
                "UPDATE interventions SET status = ? WHERE node_id = ?",
                (InterventionStatus.INVALIDATED.value, node_id),
            )
        await conn.execute(
            "UPDATE interventions SET status = ? WHERE task_id = ? AND status = ?",
            (
                InterventionStatus.INVALIDATED.value,
                event.task_id,
                InterventionStatus.PENDING.value,
            ),
        )
    elif event_type is EventType.MESSAGE_POSTED:
        await conn.execute(
            "INSERT INTO messages"
            " (id, conversation_id, seq, role, sender, text, mentions, quote_id, task_id,"
            "  node_id, intervention_id, queued_for_node_id, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                payload["message_id"],
                payload.get("conversation_id") or event.conversation_id,
                payload["seq"],
                payload["role"],
                payload.get("sender"),
                payload["text"],
                json.dumps(payload.get("mentions") or [], ensure_ascii=False),
                payload.get("quote_id"),
                payload.get("task_id") or event.task_id,
                payload.get("node_id"),
                payload.get("intervention_id"),
                payload.get("queued_for_node_id"),
                ts,
            ),
        )
    elif event_type is EventType.MESSAGE_DELIVERED:
        await conn.execute(
            "UPDATE messages SET delivered_at = ? WHERE id = ?",
            (ts, payload["message_id"]),
        )
    elif event_type is EventType.ROOM_PARTICIPANT_JOINED:
        await conn.execute(
            "INSERT INTO room_members (conversation_id, agent_name, agent_url, reason,"
            " joined_at) VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(conversation_id, agent_name) DO UPDATE SET"
            " agent_url = excluded.agent_url, reason = excluded.reason",
            (
                payload.get("conversation_id") or event.conversation_id,
                payload["agent_name"],
                payload["agent_url"],
                payload.get("reason"),
                ts,
            ),
        )
    elif event_type is EventType.ROOM_SUMMARY_UPDATED:
        await conn.execute(
            "INSERT INTO room_summaries (conversation_id, covers_seq, summary, updated_at)"
            " VALUES (?, ?, ?, ?) ON CONFLICT(conversation_id) DO UPDATE SET"
            " covers_seq = excluded.covers_seq, summary = excluded.summary,"
            " updated_at = excluded.updated_at",
            (
                payload.get("conversation_id") or event.conversation_id,
                payload["covers_seq"],
                json.dumps(payload["summary"], ensure_ascii=False),
                ts,
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
            " (id, task_id, plan_id, name, agent_url, agent_name, skill_id, deps, input,"
            "  status, attempt, requires_approval, policy_override)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
            (
                node_id,
                task_id,
                plan_id,
                node["name"],
                node.get("agent_url"),
                node.get("agent_name"),
                node.get("skill_id"),
                json.dumps(deps),
                json.dumps(node.get("input"), ensure_ascii=False),
                NodeStatus.PENDING.value,
                1 if node.get("requires_approval") else 0,
                node.get("policy_override"),
            ),
        )


async def rebuild(db: Any) -> None:
    from agent_hub.store.event_store import EventStore

    store = EventStore(db)
    async with db.transaction() as conn:
        for table in (
            "conversations",
            "nodes",
            "plans",
            "orchestration_tasks",
            "interventions",
            "checkpoints",
            "messages",
            "room_members",
            "room_summaries",
        ):
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
        conversation_id=row["conversation_id"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


def _row_to_conversation(row: aiosqlite.Row) -> Conversation:
    return Conversation(
        id=row["id"],
        title=row["title"],
        created_at=datetime.fromisoformat(row["created_at"]),
    )


def _row_to_conversation_summary(row: aiosqlite.Row) -> ConversationSummary:
    return ConversationSummary(
        id=row["id"],
        title=row["title"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        task_count=row["task_count"],
        last_status=TaskStatus(row["last_status"] or TaskStatus.PENDING.value),
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
        agent_name=row["agent_name"],
        skill_id=row["skill_id"],
        deps=json.loads(row["deps"]),
        input=json.loads(row["input"]) if row["input"] else None,
        status=NodeStatus(row["status"]),
        attempt=row["attempt"],
        requires_approval=bool(row["requires_approval"]),
        policy_override=row["policy_override"],
        a2a_task_id=row["a2a_task_id"],
        a2a_context_id=row["a2a_context_id"],
        output=json.loads(row["output"]) if row["output"] else None,
        error=row["error"],
        started_at=_parse_dt(row["started_at"]),
        ended_at=_parse_dt(row["ended_at"]),
    )


def _row_to_checkpoint(row: aiosqlite.Row) -> Checkpoint:
    return Checkpoint(
        id=row["id"],
        task_id=row["task_id"],
        seq=row["seq"],
        plan_version=row["plan_version"],
        frontier=json.loads(row["frontier"]),
        artifacts=json.loads(row["artifacts"]),
        created_at=datetime.fromisoformat(row["created_at"]),
    )


def _row_to_intervention(row: aiosqlite.Row) -> Intervention:
    return Intervention(
        id=row["id"],
        task_id=row["task_id"],
        node_id=row["node_id"],
        assigned_node_id=row["assigned_node_id"],
        assigned_to=row["assigned_to"],
        source=row["source"],
        policy=row["policy"],
        question=json.loads(row["question"]),
        answer=json.loads(row["answer"]) if row["answer"] else None,
        responder=row["responder"],
        status=InterventionStatus(row["status"]),
        deadline_at=_parse_dt(row["deadline_at"]),
        created_at=datetime.fromisoformat(row["created_at"]),
        resolved_at=_parse_dt(row["resolved_at"]),
    )


async def fetch_conversation(db: Any, conversation_id: str) -> Conversation | None:
    cursor = await db.conn.execute(
        "SELECT * FROM conversations WHERE id = ?", (conversation_id,)
    )
    row = await cursor.fetchone()
    return _row_to_conversation(row) if row else None


async def fetch_conversation_summaries(db: Any) -> list[ConversationSummary]:
    cursor = await db.conn.execute(
        "SELECT c.id, c.title, c.created_at,"
        "       COALESCE(MAX(t.updated_at), c.created_at) AS updated_at,"
        "       COUNT(t.id) AS task_count,"
        "       (SELECT status FROM orchestration_tasks"
        "         WHERE conversation_id = c.id"
        "         ORDER BY created_at DESC, id DESC LIMIT 1) AS last_status"
        " FROM conversations c"
        " LEFT JOIN orchestration_tasks t ON t.conversation_id = c.id"
        " GROUP BY c.id, c.title, c.created_at"
        " ORDER BY updated_at DESC, c.created_at DESC"
    )
    return [_row_to_conversation_summary(row) for row in await cursor.fetchall()]


async def fetch_task_ids_for_conversation(db: Any, conversation_id: str) -> list[str]:
    cursor = await db.conn.execute(
        "SELECT id FROM orchestration_tasks WHERE conversation_id = ?"
        " ORDER BY created_at, id",
        (conversation_id,),
    )
    return [row["id"] for row in await cursor.fetchall()]


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


async def fetch_intervention(db: Any, intervention_id: str) -> Intervention | None:
    cursor = await db.conn.execute(
        "SELECT * FROM interventions WHERE id = ?", (intervention_id,)
    )
    row = await cursor.fetchone()
    return _row_to_intervention(row) if row else None


async def fetch_interventions(
    db: Any, task_id: str, status: InterventionStatus | None = None
) -> list[Intervention]:
    if status is None:
        cursor = await db.conn.execute(
            "SELECT * FROM interventions WHERE task_id = ? ORDER BY created_at, id",
            (task_id,),
        )
    else:
        cursor = await db.conn.execute(
            "SELECT * FROM interventions WHERE task_id = ? AND status = ?"
            " ORDER BY created_at, id",
            (task_id, status.value),
        )
    return [_row_to_intervention(row) for row in await cursor.fetchall()]


async def fetch_interventions_for_node(db: Any, node_id: str) -> list[Intervention]:
    cursor = await db.conn.execute(
        "SELECT * FROM interventions WHERE node_id = ? ORDER BY created_at, id",
        (node_id,),
    )
    return [_row_to_intervention(row) for row in await cursor.fetchall()]


async def fetch_interventions_by_assigned_node(
    db: Any, node_id: str
) -> list[Intervention]:
    cursor = await db.conn.execute(
        "SELECT * FROM interventions WHERE assigned_node_id = ? ORDER BY created_at, id",
        (node_id,),
    )
    return [_row_to_intervention(row) for row in await cursor.fetchall()]


async def fetch_checkpoint(db: Any, checkpoint_id: str) -> Checkpoint | None:
    cursor = await db.conn.execute(
        "SELECT * FROM checkpoints WHERE id = ?", (checkpoint_id,)
    )
    row = await cursor.fetchone()
    return _row_to_checkpoint(row) if row else None


async def fetch_checkpoints(db: Any, task_id: str) -> list[Checkpoint]:
    cursor = await db.conn.execute(
        "SELECT * FROM checkpoints WHERE task_id = ? ORDER BY seq, id", (task_id,)
    )
    return [_row_to_checkpoint(row) for row in await cursor.fetchall()]


def _row_to_room_message(row: aiosqlite.Row) -> RoomMessage:
    return RoomMessage(
        id=row["id"],
        conversation_id=row["conversation_id"],
        seq=row["seq"],
        role=row["role"],
        sender=row["sender"],
        text=row["text"],
        mentions=json.loads(row["mentions"]),
        quote_id=row["quote_id"],
        task_id=row["task_id"],
        node_id=row["node_id"],
        intervention_id=row["intervention_id"],
        queued_for_node_id=row["queued_for_node_id"],
        delivered_at=_parse_dt(row["delivered_at"]),
        created_at=datetime.fromisoformat(row["created_at"]),
    )


def _row_to_room_member(row: aiosqlite.Row) -> RoomMember:
    return RoomMember(
        conversation_id=row["conversation_id"],
        agent_name=row["agent_name"],
        agent_url=row["agent_url"],
        reason=row["reason"],
        joined_at=datetime.fromisoformat(row["joined_at"]),
    )


def _row_to_room_summary(row: aiosqlite.Row) -> RoomSummary:
    return RoomSummary(
        conversation_id=row["conversation_id"],
        covers_seq=row["covers_seq"],
        summary=json.loads(row["summary"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
    )


async def next_message_seq(db: Any, conversation_id: str) -> int:
    cursor = await db.conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS seq FROM messages WHERE conversation_id = ?",
        (conversation_id,),
    )
    return int((await cursor.fetchone())["seq"])


async def fetch_messages(
    db: Any, conversation_id: str, after_seq: int = 0, limit: int = 200
) -> list[RoomMessage]:
    cursor = await db.conn.execute(
        "SELECT * FROM messages WHERE conversation_id = ? AND seq > ?"
        " ORDER BY seq LIMIT ?",
        (conversation_id, after_seq, limit),
    )
    return [_row_to_room_message(row) for row in await cursor.fetchall()]


async def fetch_message(db: Any, message_id: str) -> RoomMessage | None:
    cursor = await db.conn.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
    row = await cursor.fetchone()
    return _row_to_room_message(row) if row else None


async def fetch_messages_for_node(db: Any, node_id: str) -> list[RoomMessage]:
    cursor = await db.conn.execute(
        "SELECT * FROM messages WHERE node_id = ? ORDER BY seq", (node_id,)
    )
    return [_row_to_room_message(row) for row in await cursor.fetchall()]


async def fetch_room_members(db: Any, conversation_id: str) -> list[RoomMember]:
    cursor = await db.conn.execute(
        "SELECT * FROM room_members WHERE conversation_id = ?"
        " ORDER BY joined_at, agent_name",
        (conversation_id,),
    )
    return [_row_to_room_member(row) for row in await cursor.fetchall()]


async def fetch_room_summary(db: Any, conversation_id: str) -> RoomSummary | None:
    cursor = await db.conn.execute(
        "SELECT * FROM room_summaries WHERE conversation_id = ?", (conversation_id,)
    )
    row = await cursor.fetchone()
    return _row_to_room_summary(row) if row else None
