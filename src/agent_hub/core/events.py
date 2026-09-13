from __future__ import annotations

import asyncio

from agent_hub.store.event_store import Event

_CLOSED = object()


class SubscriptionClosed(RuntimeError):
    pass


class EventSubscription:
    def __init__(self, key: str, maxsize: int):
        self.key = key
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._closed = False

    def offer(self, event: Event) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._replace_with_closed()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._queue.put_nowait(_CLOSED)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                self._queue.put_nowait(_CLOSED)
            except asyncio.QueueFull:
                pass

    def _replace_with_closed(self) -> None:
        self._closed = True
        self._queue = asyncio.Queue()
        self._queue.put_nowait(_CLOSED)

    async def get(self) -> Event:
        item = await self._queue.get()
        if item is _CLOSED:
            raise SubscriptionClosed()
        return item


class EventBus:
    def __init__(self, max_queue_size: int = 256):
        self._max_queue_size = max_queue_size
        self._subscriptions: dict[str, set[EventSubscription]] = {}

    def subscribe(self, key: str) -> EventSubscription:
        subscription = EventSubscription(key, self._max_queue_size)
        self._subscriptions.setdefault(key, set()).add(subscription)
        return subscription

    def unsubscribe(self, subscription: EventSubscription) -> None:
        subs = self._subscriptions.get(subscription.key)
        if subs is not None:
            subs.discard(subscription)
            if not subs:
                self._subscriptions.pop(subscription.key, None)

    def publish(self, event: Event) -> None:
        keys: list[str] = []
        if event.task_id:
            keys.append(event.task_id)
        if event.conversation_id:
            keys.append(f"room:{event.conversation_id}")
        for key in keys:
            for subscription in list(self._subscriptions.get(key, ())):
                subscription.offer(event)

    def close_all(self) -> None:
        for subs in list(self._subscriptions.values()):
            for subscription in list(subs):
                subscription.close()
        self._subscriptions.clear()
