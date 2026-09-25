from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypedDict

from a2a.helpers import new_task
from a2a.server.context import ServerCallContext
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import TaskState
from fastapi import APIRouter, Depends, HTTPException
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import JsonValue

from choirworks.a2a.tasks import list_all_tasks
from choirworks.api.deps import get_context_store, get_session_manager, get_task_store
from choirworks.api.replay import synthesize_replay_events
from choirworks.orchestration.rewind import (
    REWIND_KEY,
    RewindUnavailable,
    is_human_turn,
    restore_state,
)
from choirworks.orchestration.state import (
    OrchestrationState,
    state_from_json,
    state_to_json,
)
from choirworks.store.contexts import ContextStore

if TYPE_CHECKING:
    from choirworks.orchestration.session import SessionManager

router = APIRouter(tags=["conversations"])


class ConversationSummary(TypedDict):
    id: str
    title: str
    created_at: str
    updated_at: str
    task_count: int
    last_status: str


class ConversationPayload(TypedDict):
    id: str
    context: JsonValue | None
    tasks: list[dict[str, JsonValue]]


class CreateConversationResponse(TypedDict):
    conversation_id: str
    title: str


async def _conversation_payload(
    context_id: str,
    task_store: TaskStore,
    context_store: ContextStore,
) -> ConversationPayload:
    tasks = await list_all_tasks(task_store, context_id=context_id, reverse=True)
    if not tasks:
        raise HTTPException(status_code=404, detail="conversation not found")
    record = await context_store.get(context_id)
    context: JsonValue | None = None
    if record is not None:
        try:
            context = json.loads(record.state)
        except ValueError:
            context = None
    visible_dicts = [MessageToDict(task, preserving_proto_field_name=True) for task in tasks]
    return {"id": context_id, "context": context, "tasks": visible_dicts}


@router.post("/conversations")
async def create_conversation(
    body: dict[str, Any],
    context_store: ContextStore = Depends(get_context_store),
) -> CreateConversationResponse:
    conversation_id = uuid.uuid4().hex
    title = str(body.get("title", ""))
    await context_store.create(conversation_id, title=title)
    return {"conversation_id": conversation_id, "title": title}


@router.get("/conversations")
async def list_conversations(
    task_store: TaskStore = Depends(get_task_store),
    context_store: ContextStore = Depends(get_context_store),
) -> list[ConversationSummary]:
    records = {record.context_id: record for record in await context_store.list()}
    sessions: dict[str, ConversationSummary] = {
        context_id: {
            "id": context_id,
            "title": record.title,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "task_count": 0,
            "last_status": "",
        }
        for context_id, record in records.items()
    }
    latest_state: dict[str, TaskState] = {
        context_id: TaskState.TASK_STATE_UNSPECIFIED for context_id in sessions
    }
    tasks = await list_all_tasks(task_store)
    for task in tasks:
        ctx_id = task.context_id
        state_name = TaskState.Name(task.status.state).replace("TASK_STATE_", "").lower()
        if ctx_id not in sessions:
            title = ""
            if task.metadata.fields:
                meta = MessageToDict(task.metadata, preserving_proto_field_name=True)
                if meta.get("title"):
                    title = meta["title"]
            sessions[ctx_id] = {
                "id": ctx_id,
                "title": title,
                "created_at": "",
                "updated_at": "",
                "task_count": 0,
                "last_status": state_name,
            }
            latest_state[ctx_id] = task.status.state
        session = sessions[ctx_id]
        session["task_count"] += 1
        if task.status.state > latest_state[ctx_id]:
            latest_state[ctx_id] = task.status.state
            session["last_status"] = state_name
    return list(sessions.values())


@router.get("/conversations/{context_id}")
async def get_conversation(
    context_id: str,
    task_store: TaskStore = Depends(get_task_store),
    context_store: ContextStore = Depends(get_context_store),
) -> ConversationPayload:
    return await _conversation_payload(context_id, task_store, context_store)


@router.get("/conversations/{context_id}/replay")
async def replay_conversation(
    context_id: str,
    task_store: TaskStore = Depends(get_task_store),
    context_store: ContextStore = Depends(get_context_store),
) -> list[dict[str, object]]:
    tasks = await list_all_tasks(task_store, context_id=context_id, reverse=True)
    if not tasks:
        raise HTTPException(status_code=404, detail="conversation not found")
    record = await context_store.get(context_id)
    state: OrchestrationState | None = None
    if record is not None:
        try:
            state = state_from_json(record.state)
        except (ValueError, TypeError):
            state = None
    return synthesize_replay_events(tasks, context_id, state)


@router.post("/conversations/{context_id}/rewind")
async def rewind_conversation(
    context_id: str,
    body: dict[str, Any],
    task_store: TaskStore = Depends(get_task_store),
    context_store: ContextStore = Depends(get_context_store),
    session_mgr: SessionManager = Depends(get_session_manager),
) -> ConversationPayload:
    task_id = str(body.get("task_id", ""))
    if not task_id:
        raise HTTPException(status_code=400, detail="task_id is required")
    if session_mgr.session_is_active(context_id):
        raise HTTPException(status_code=409, detail="会话正在执行中，无法回退")
    record = await context_store.get(context_id)
    if record is None:
        raise HTTPException(status_code=404, detail="conversation not found")
    tasks = await list_all_tasks(task_store, context_id=context_id, reverse=True)
    if not tasks:
        raise HTTPException(status_code=404, detail="conversation not found")
    index = {task.id: position for position, task in enumerate(tasks)}
    if task_id not in index:
        raise HTTPException(status_code=404, detail="task not found")
    if not is_human_turn(tasks[index[task_id]]):
        raise HTTPException(status_code=400, detail="只有人类消息开启的回合可以回退")
    try:
        state = restore_state(tasks, task_id)
    except RewindUnavailable as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await context_store.upsert_state(context_id, state_to_json(state))
    now = Timestamp()
    now.FromDatetime(datetime.now(UTC))
    marker = new_task(
        task_id=uuid.uuid4().hex,
        context_id=context_id,
        state=TaskState.TASK_STATE_COMPLETED,
    )
    marker.status.timestamp.CopyFrom(now)
    ParseDict({REWIND_KEY: task_id}, marker.metadata)
    await task_store.save(marker, ServerCallContext())
    session_mgr.evict_session(context_id)
    return await _conversation_payload(context_id, task_store, context_store)
