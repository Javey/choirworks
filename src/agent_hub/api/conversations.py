from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from agent_hub.api.schemas import (
    ConversationDetail,
    CreateConversationIn,
    CreateConversationOut,
)
from agent_hub.models.domain import ConversationSummary
from agent_hub.store import projections

router = APIRouter(tags=["conversations"])


@router.post(
    "/conversations", status_code=201, response_model=CreateConversationOut
)
async def create_conversation(
    body: CreateConversationIn, request: Request
) -> CreateConversationOut:
    coordinator = request.app.state.coordinator
    conversation_id = await coordinator.create_conversation(body.title)
    conversation = await projections.fetch_conversation(
        request.app.state.db, conversation_id
    )
    assert conversation is not None
    return CreateConversationOut(
        conversation_id=conversation_id, title=conversation.title
    )


@router.get("/conversations", response_model=list[ConversationSummary])
async def list_conversations(request: Request) -> list[ConversationSummary]:
    return await projections.fetch_conversation_summaries(request.app.state.db)


@router.get("/conversations/{conversation_id}", response_model=ConversationDetail)
async def get_conversation(conversation_id: str, request: Request) -> ConversationDetail:
    db = request.app.state.db
    summaries = await projections.fetch_conversation_summaries(db)
    summary = next((item for item in summaries if item.id == conversation_id), None)
    if summary is None:
        raise HTTPException(
            status_code=404, detail=f"conversation not found: {conversation_id}"
        )
    task_ids = await projections.fetch_task_ids_for_conversation(db, conversation_id)
    service = request.app.state.task_service
    tasks = [await service.get_snapshot(task_id) for task_id in task_ids]
    return ConversationDetail(conversation=summary, tasks=tasks)
