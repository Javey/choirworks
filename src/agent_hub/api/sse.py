from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from agent_hub.core.events import SubscriptionClosed
from agent_hub.core.tasks import TaskNotFound
from agent_hub.models.enums import EventType
from agent_hub.store import projections

router = APIRouter(tags=["events"])


def format_event(event_type: EventType, seq: int, payload: dict) -> dict[str, str]:
    return {
        "event": event_type.value,
        "id": str(seq),
        "data": json.dumps(payload, ensure_ascii=False),
    }


@router.get("/tasks/{task_id}/events")
async def task_events(task_id: str, request: Request, after_seq: int = 0):
    service = request.app.state.task_service
    try:
        await service.get_snapshot(task_id)
    except TaskNotFound as exc:
        raise HTTPException(status_code=404, detail=f"task not found: {task_id}") from exc

    store = request.app.state.event_store
    bus = request.app.state.event_bus
    last_event_id = request.headers.get("last-event-id")
    start = after_seq
    if last_event_id is not None and last_event_id.isdigit():
        start = max(start, int(last_event_id))

    async def stream():
        subscription = bus.subscribe(task_id)
        try:
            seen = start
            for event in await store.replay(task_id, start):
                seen = event.seq
                yield format_event(event.type, event.seq, event.payload)
            while True:
                try:
                    event = await subscription.get()
                except SubscriptionClosed:
                    break
                if event.seq <= seen:
                    continue
                seen = event.seq
                yield format_event(event.type, event.seq, event.payload)
        finally:
            subscription.close()
            bus.unsubscribe(subscription)

    return EventSourceResponse(stream(), ping=15)


@router.get("/conversations/{conversation_id}/stream")
async def conversation_events(
    conversation_id: str, request: Request, since_seq: int = 0
):
    if (
        await projections.fetch_conversation(request.app.state.db, conversation_id)
        is None
    ):
        raise HTTPException(
            status_code=404, detail=f"conversation not found: {conversation_id}"
        )
    store = request.app.state.event_store
    bus = request.app.state.event_bus
    last_event_id = request.headers.get("last-event-id")
    start = since_seq
    if last_event_id is not None and last_event_id.isdigit():
        start = max(start, int(last_event_id))
    key = f"room:{conversation_id}"

    async def stream():
        subscription = bus.subscribe(key)
        try:
            seen = start
            for event in await store.replay_conversation(conversation_id, start):
                seen = event.seq
                yield format_event(event.type, event.seq, event.payload)
            while True:
                try:
                    event = await subscription.get()
                except SubscriptionClosed:
                    break
                if event.seq <= seen:
                    continue
                seen = event.seq
                yield format_event(event.type, event.seq, event.payload)
        finally:
            subscription.close()
            bus.unsubscribe(subscription)

    return EventSourceResponse(stream(), ping=15)
