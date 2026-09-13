from choirworks.core.room import post_message
from choirworks.store import projections
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore


async def make_room(tmp_path):
    db = Database(tmp_path / "room.db")
    await db.initialize()
    return db, EventStore(db)


async def test_post_message_allocates_room_seq(tmp_path):
    db, store = await make_room(tmp_path)
    try:
        first = await post_message(
            db, store, conversation_id="c1", role="user", sender="CEO", text="第一问"
        )
        second = await post_message(
            db,
            store,
            conversation_id="c1",
            role="agent",
            sender="researcher",
            text="答复",
            mentions=["writer"],
            quote_id=first.id,
            node_id="p1:n1",
        )
        assert (first.seq, second.seq) == (1, 2)
        assert second.mentions == ["writer"]
        assert second.quote_id == first.id
        assert second.node_id == "p1:n1"
        assert await projections.next_message_seq(db, "c1") == 2

        other = await post_message(
            db, store, conversation_id="c2", role="user", sender="CEO", text="另一个群"
        )
        assert other.seq == 1
    finally:
        await db.close()
