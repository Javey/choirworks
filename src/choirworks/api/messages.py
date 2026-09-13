from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from choirworks.api.schemas import PostMessageIn, PostMessageOut, RoomMessagesOut
from choirworks.store import projections

router = APIRouter(tags=["messages"])


@router.get("/conversations/{conversation_id}/messages", response_model=RoomMessagesOut)
async def list_messages(
    conversation_id: str,
    request: Request,
    since_seq: int = 0,
    limit: int = 200,
) -> RoomMessagesOut:
    db = request.app.state.db
    if await projections.fetch_conversation(db, conversation_id) is None:
        raise HTTPException(
            status_code=404, detail=f"conversation not found: {conversation_id}"
        )
    messages = await projections.fetch_messages(
        db, conversation_id, after_seq=since_seq, limit=limit
    )
    members = await projections.fetch_room_members(db, conversation_id)
    summary = await projections.fetch_room_summary(db, conversation_id)
    last_seq = (
        messages[-1].seq
        if messages
        else await projections.next_message_seq(db, conversation_id)
    )
    return RoomMessagesOut(
        messages=messages, members=members, summary=summary, last_seq=last_seq
    )


@router.post(
    "/conversations/{conversation_id}/messages",
    status_code=201,
    response_model=PostMessageOut,
)
async def create_message(
    conversation_id: str, body: PostMessageIn, request: Request
) -> PostMessageOut:
    db = request.app.state.db
    registry = request.app.state.registry
    if await projections.fetch_conversation(db, conversation_id) is None:
        raise HTTPException(
            status_code=404, detail=f"conversation not found: {conversation_id}"
        )
    if body.interrupt and body.quote_id is None:
        raise HTTPException(status_code=400, detail="打断需要引用一条消息")
    for agent_name in body.mentions:
        if await registry.get_by_name(agent_name) is None:
            raise HTTPException(
                status_code=400, detail=f"agent not registered: {agent_name}"
            )
    try:
        result = await request.app.state.coordinator.handle_human_message(
            conversation_id,
            text=body.text,
            mentions=body.mentions,
            quote_id=body.quote_id,
            interrupt=body.interrupt,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return PostMessageOut(
        message_id=result.message.id, seq=result.message.seq, task_id=result.task_id
    )
