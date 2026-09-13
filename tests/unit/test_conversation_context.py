from choirworks.core.conversations import build_conversation_context
from choirworks.models.enums import EventType, NodeStatus, TaskStatus
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore


async def seed_task(
    db: Database,
    store: EventStore,
    task_id: str,
    conversation_id: str,
    request: str,
    *,
    completed: bool = True,
    result: str = "done",
    title: str | None = None,
) -> None:
    await store.append(
        task_id,
        EventType.TASK_CREATED,
        {
            "request": request,
            "policy": None,
            "conversation_id": conversation_id,
            "conversation_title": title,
        },
    )
    plan_id = f"{task_id}-p1"
    node_id = f"{plan_id}:n1"
    await store.append(
        task_id,
        EventType.PLAN_CREATED,
        {
            "plan_id": plan_id,
            "version": 1,
            "rationale": "seed",
            "dag": {
                "nodes": [
                    {
                        "id": "n1",
                        "name": "n1",
                        "agent_url": "http://agent",
                        "agent_name": "echo",
                        "deps": [],
                        "input": {"text": request},
                    }
                ]
            },
        },
    )
    await store.append(
        task_id,
        EventType.NODE_DISPATCHED,
        {"node_id": node_id, "a2a_task_id": f"remote-{task_id}", "a2a_context_id": task_id},
    )
    await store.append(
        task_id,
        EventType.NODE_OUTPUT,
        {
            "node_id": node_id,
            "output": {"artifacts": [{"id": "message", "name": "message", "text": result}]},
        },
    )
    await store.append(
        task_id,
        EventType.NODE_STATE_CHANGED,
        {"node_id": node_id, "from": NodeStatus.WORKING.value, "to": NodeStatus.COMPLETED.value},
    )
    if completed:
        await store.append(task_id, EventType.TASK_COMPLETED, {})
    else:
        await store.append(
            task_id,
            EventType.TASK_STATE_CHANGED,
            {"from": TaskStatus.PLANNING.value, "to": TaskStatus.RUNNING.value},
        )


async def test_builds_history_from_terminal_tasks_only(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await seed_task(db, store, "t1", "c1", "第一问", result="答案一", title="第一问")
        await seed_task(db, store, "t2", "c1", "追问一", result="答案二")
        await seed_task(db, store, "t3", "c1", "未完成", completed=False)

        context = await build_conversation_context(db, "c1", exclude_task_id=None)
        assert context is not None
        assert "User: 第一问" in context
        assert "Result: 答案一" in context
        assert "User: 追问一" in context
        assert "Result: 答案二" in context
        assert "未完成" not in context
    finally:
        await db.close()


async def test_exclude_current_task(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await seed_task(db, store, "t1", "c1", "第一问", result="答案一")
        await seed_task(db, store, "t2", "c1", "追问一", result="答案二")

        context = await build_conversation_context(db, "c1", exclude_task_id="t2")
        assert context is not None
        assert "第一问" in context
        assert "追问一" not in context
    finally:
        await db.close()


async def test_max_chars_drops_oldest(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        for index in range(4):
            await seed_task(
                db,
                store,
                f"t{index}",
                "c1",
                f"请求{index}",
                result="长" * 3000,
            )

        context = await build_conversation_context(db, "c1", exclude_task_id=None)
        assert context is not None
        assert len(context) <= 4000
        assert "请求0" not in context
        assert "请求3" in context
    finally:
        await db.close()


async def test_no_history_returns_none(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await seed_task(db, store, "t1", "c1", "唯一", completed=False)
        context = await build_conversation_context(db, "c1", exclude_task_id=None)
        assert context is None
    finally:
        await db.close()
