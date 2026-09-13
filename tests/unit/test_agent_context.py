from choirworks.core.context import build_agent_context
from choirworks.core.room import post_message
from choirworks.models.enums import EventType
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore


async def make_room(tmp_path):
    db = Database(tmp_path / "ctx.db")
    await db.initialize()
    events = EventStore(db)
    await events.append(
        "t1",
        EventType.TASK_CREATED,
        {
            "request": "调研 A2A",
            "policy": None,
            "conversation_id": "c1",
            "conversation_title": "调研 A2A",
        },
    )
    return db, events


async def test_small_room_is_fully_included(tmp_path):
    db, events = await make_room(tmp_path)
    try:
        first = await post_message(
            db, events, conversation_id="c1", role="user", sender="CEO", text="大家好"
        )
        second = await post_message(
            db, events, conversation_id="c1", role="user", sender="CEO", text="第二句"
        )
        package = await build_agent_context(
            db, "c1", "researcher", "请调研", budget=8000, recent_window=5
        )
        assert package.included_message_ids == [first.id, second.id]
        assert package.truncated is False
        assert "调研 A2A" in package.text
        assert "大家好" in package.text
        assert "请调研" in package.text
    finally:
        await db.close()


async def test_mention_beyond_window_and_quote_are_included(tmp_path):
    db, events = await make_room(tmp_path)
    try:
        question = await post_message(
            db,
            events,
            conversation_id="c1",
            role="agent",
            sender="researcher",
            text="谁能补充数据？",
        )
        mentioned = await post_message(
            db,
            events,
            conversation_id="c1",
            role="agent",
            sender="writer",
            text="@researcher 请补充数据",
            mentions=["researcher"],
        )
        quoted = await post_message(
            db,
            events,
            conversation_id="c1",
            role="agent",
            sender="writer",
            text="这个问题需要你回答",
            quote_id=question.id,
        )
        for index in range(25):
            await post_message(
                db,
                events,
                conversation_id="c1",
                role="user",
                sender="CEO",
                text=f"闲聊 {index}",
            )
        package = await build_agent_context(
            db, "c1", "researcher", "补充数据", budget=8000, recent_window=5
        )
        assert mentioned.id in package.included_message_ids
        assert quoted.id in package.included_message_ids
        assert question.id in package.included_message_ids
    finally:
        await db.close()


async def test_budget_drops_recent_but_keeps_mention(tmp_path):
    db, events = await make_room(tmp_path)
    try:
        mention = await post_message(
            db,
            events,
            conversation_id="c1",
            role="user",
            sender="CEO",
            text="@researcher 关键请求",
            mentions=["researcher"],
        )
        for index in range(30):
            await post_message(
                db,
                events,
                conversation_id="c1",
                role="user",
                sender="CEO",
                text=f"很长的闲聊内容 {'x' * 200} {index}",
            )
        package = await build_agent_context(
            db, "c1", "researcher", "处理关键请求", budget=200, recent_window=20
        )
        assert mention.id in package.included_message_ids
        assert len(package.included_message_ids) < 31
        assert package.truncated is True
    finally:
        await db.close()
