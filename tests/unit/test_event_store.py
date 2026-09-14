from typing import Any

from choirworks.models.enums import EventType, NodeStatus, TaskStatus
from choirworks.store import projections
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore


async def make_store(tmp_path) -> tuple[Database, EventStore]:
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    return db, EventStore(db)


async def seed_task(store: EventStore) -> tuple[str, str]:
    task_id = "t1"
    plan_id = "p1"
    await store.append(task_id, EventType.TASK_CREATED, {"request": "hi", "policy": None})
    await store.append(
        task_id,
        EventType.PLAN_CREATED,
        {
            "plan_id": plan_id,
            "version": 1,
            "rationale": "manual",
            "dag": {
                "nodes": [
                    {
                        "id": "n1",
                        "name": "step",
                        "agent_url": "http://agent",
                        "skill_id": None,
                        "deps": [],
                        "input": {"text": "hi"},
                        "requires_approval": False,
                        "policy_override": None,
                    }
                ]
            },
        },
    )
    await store.append(
        task_id,
        EventType.TASK_STATE_CHANGED,
        {"from": TaskStatus.PLANNING.value, "to": TaskStatus.RUNNING.value},
    )
    return task_id, plan_id


async def test_append_assigns_increasing_seq_and_projects(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        task_id, plan_id = await seed_task(store)
        task = await projections.fetch_task(db, task_id)
        assert task is not None
        assert task.status is TaskStatus.RUNNING
        assert task.plan_version == 1

        plan = await projections.fetch_current_plan(db, task_id)
        assert plan is not None and plan.id == plan_id

        nodes = await projections.fetch_nodes(db, task_id)
        assert len(nodes) == 1
        assert nodes[0].id == f"{plan_id}:n1"
        assert nodes[0].status is NodeStatus.PENDING

        event = await store.append(task_id, EventType.NODE_DISPATCH_INTENT, {
            "node_id": f"{plan_id}:n1", "message_id": "m1", "attempt": 1,
        })
        assert event.seq > 0
        node = await projections.fetch_node(db, f"{plan_id}:n1")
        assert node is not None and node.attempt == 1
    finally:
        await db.close()


async def test_replay_after_seq(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        task_id, _ = await seed_task(store)
        events = await store.replay(task_id)
        assert len(events) == 3
        later = await store.replay(task_id, after_seq=events[0].seq)
        assert len(later) == 2
        assert await store.latest_seq(task_id) == events[-1].seq
    finally:
        await db.close()


async def test_rebuild_projections_is_deterministic(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        task_id, plan_id = await seed_task(store)
        node_id = f"{plan_id}:n1"
        await store.append(task_id, EventType.NODE_DISPATCH_INTENT, {
            "node_id": node_id, "message_id": "m1", "attempt": 1,
        })
        await store.append(task_id, EventType.NODE_DISPATCHED, {
            "node_id": node_id, "a2a_task_id": "r1", "a2a_context_id": task_id,
            "message_id": "m1",
        })
        await store.append(task_id, EventType.NODE_STATE_CHANGED, {
            "node_id": node_id, "from": NodeStatus.DISPATCHED.value,
            "to": NodeStatus.WORKING.value,
        })
        await store.append(task_id, EventType.NODE_STATE_CHANGED, {
            "node_id": node_id, "from": NodeStatus.WORKING.value,
            "to": NodeStatus.COMPLETED.value,
        })
        await store.append(task_id, EventType.NODE_OUTPUT, {
            "node_id": node_id, "output": {"artifacts": [{"text": "ok"}]},
        })
        await store.append(task_id, EventType.TASK_COMPLETED, {})

        before = await snapshot(db)
        await projections.rebuild(db)
        after = await snapshot(db)
        assert before == after

        task = await projections.fetch_task(db, task_id)
        assert task is not None and task.status is TaskStatus.COMPLETED
        node = await projections.fetch_node(db, node_id)
        assert node is not None and node.status is NodeStatus.COMPLETED
        assert node.output == {"artifacts": [{"text": "ok"}]}
    finally:
        await db.close()


async def test_latest_conversation_seq(tmp_path):
    db, store = await make_store(tmp_path)
    try:
        await store.append(
            None,
            EventType.CONVERSATION_CREATED,
            {"conversation_id": "c1", "title": "群"},
            conversation_id="c1",
        )
        posted = await store.append(
            None,
            EventType.MESSAGE_POSTED,
            {
                "message_id": "m1",
                "conversation_id": "c1",
                "seq": 1,
                "role": "user",
                "text": "hi",
            },
            conversation_id="c1",
        )
        await store.append("t1", EventType.TASK_COMPLETED, {})
        assert await store.latest_conversation_seq("c1") == posted.seq
        assert await store.latest_conversation_seq("missing") == 0
    finally:
        await db.close()


async def snapshot(db: Database) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for table in (
        "orchestration_tasks",
        "plans",
        "nodes",
        "interventions",
        "checkpoints",
        "events",
    ):
        cursor = await db.conn.execute(f"SELECT * FROM {table} ORDER BY 1")
        result[table] = [dict(row) for row in await cursor.fetchall()]
    return result
