import asyncio
from datetime import UTC, datetime

import pytest

from agent_hub.core.events import EventBus, SubscriptionClosed
from agent_hub.models.enums import EventType
from agent_hub.store.event_store import Event


def make_event(seq: int, task_id: str = "t1") -> Event:
    return Event(
        seq=seq,
        task_id=task_id,
        type=EventType.NODE_STATE_CHANGED,
        payload={"seq": seq},
        created_at=datetime.now(UTC),
    )


async def test_publish_delivers_to_subscriber():
    bus = EventBus()
    sub = bus.subscribe("t1")
    bus.publish(make_event(1))
    bus.publish(make_event(2))
    assert (await sub.get()).seq == 1
    assert (await sub.get()).seq == 2


async def test_subscribers_are_isolated_by_task():
    bus = EventBus()
    sub = bus.subscribe("t1")
    bus.publish(make_event(1, task_id="t2"))
    bus.publish(make_event(2, task_id="t1"))
    assert (await sub.get()).seq == 2


async def test_slow_consumer_is_closed():
    bus = EventBus(max_queue_size=2)
    sub = bus.subscribe("t1")
    for seq in range(5):
        bus.publish(make_event(seq))
    with pytest.raises(SubscriptionClosed):
        await sub.get()


async def test_close_unblocks_waiting_get():
    bus = EventBus()
    sub = bus.subscribe("t1")

    async def wait() -> None:
        await sub.get()

    waiter = asyncio.create_task(wait())
    await asyncio.sleep(0.01)
    sub.close()
    with pytest.raises(SubscriptionClosed):
        await waiter
