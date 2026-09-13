from choirworks.core.room import post_message
from choirworks.core.summary import SummaryDraft, maybe_update_summary
from choirworks.models.enums import EventType
from choirworks.store import projections
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore
from tests.support.fakes import FakeLLM


async def make_room(tmp_path):
    db = Database(tmp_path / "summary.db")
    await db.initialize()
    events = EventStore(db)
    await events.append(
        "t1",
        EventType.TASK_CREATED,
        {
            "request": "目标",
            "policy": None,
            "conversation_id": "c1",
            "conversation_title": "目标",
        },
    )
    return db, events


async def seed_messages(db, events, count: int, prefix: str = "消息") -> None:
    for index in range(count):
        await post_message(
            db,
            events,
            conversation_id="c1",
            role="user",
            sender="CEO",
            text=f"{prefix} {index + 1}",
        )


async def test_summary_triggers_after_twelve_messages(tmp_path):
    db, events = await make_room(tmp_path)
    try:
        await seed_messages(db, events, 12)
        draft = SummaryDraft(goal="目标", decisions=["决策一"], todos=["待办一"])
        summary = await maybe_update_summary(db, events, FakeLLM([draft]), "c1")
        assert summary is not None
        assert summary.covers_seq == 12
        assert summary.summary["goal"] == "目标"
        assert summary.summary["decisions"] == ["决策一"]
        assert await maybe_update_summary(db, events, FakeLLM([draft]), "c1") is None
    finally:
        await db.close()


async def test_summary_failure_keeps_previous(tmp_path):
    db, events = await make_room(tmp_path)
    try:
        await seed_messages(db, events, 12)
        llm = FakeLLM([RuntimeError("llm down")])
        assert await maybe_update_summary(db, events, llm, "c1") is None
        assert await projections.fetch_room_summary(db, "c1") is None
        errors = [
            event
            for event in await events.replay_conversation("c1")
            if event.type is EventType.ERROR
        ]
        assert any("摘要" in (event.payload.get("message") or "") for event in errors)
    finally:
        await db.close()
