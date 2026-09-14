from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import Any

from a2a.server.context import ServerCallContext
from a2a.server.request_handlers import RequestHandler
from a2a.types import (
    CancelTaskRequest,
    GetTaskRequest,
    ListTasksRequest,
    ListTasksResponse,
    Message,
    SendMessageRequest,
    StreamResponse,
    SubscribeToTaskRequest,
    Task,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)
from a2a.utils.errors import (
    InternalError,
    InvalidParamsError,
    TaskNotCancelableError,
    TaskNotFoundError,
    UnsupportedOperationError,
)
from fastapi import FastAPI

from choirworks.a2a.mapping import (
    A2A_ROOM_URI,
    TaskStreamMapper,
    room_message_to_a2a,
    room_to_task,
    snapshot_to_task,
)
from choirworks.core.cancel import TaskNotCancelable, cancel_task
from choirworks.core.events import SubscriptionClosed
from choirworks.core.room import post_message
from choirworks.core.tasks import TaskNotFound, TaskSnapshot
from choirworks.models.enums import (
    TERMINAL_TASK_STATUSES,
    EventType,
    InterventionStatus,
    NodeStatus,
    TaskStatus,
)
from choirworks.store import projections
from choirworks.store.event_store import Event

logger = logging.getLogger(__name__)


def _unwrap_stream_response(response: StreamResponse) -> Any:
    field = response.WhichOneof("payload")
    if field is None:
        raise InternalError("stream response has no payload")
    return getattr(response, field)


class HubA2AHandler(RequestHandler):
    def __init__(self, app: FastAPI):
        self._app = app

    async def _snapshot(self, task_id: str) -> TaskSnapshot:
        return await self._app.state.task_service.get_snapshot(task_id)

    async def _pending_question(self, snapshot: TaskSnapshot) -> str | None:
        if snapshot.task.status is not TaskStatus.AWAITING_INPUT:
            return None
        rows = await projections.fetch_interventions(
            self._app.state.db, snapshot.task.id, InterventionStatus.PENDING
        )
        if not rows:
            return None
        return (rows[-1].question or {}).get("text")

    async def _a2a_task(self, task_id: str) -> Task:
        snapshot = await self._snapshot(task_id)
        return snapshot_to_task(snapshot, question=await self._pending_question(snapshot))

    def _note_extensions(self, context: ServerCallContext) -> None:
        if A2A_ROOM_URI in context.requested_extensions:
            logger.debug("A2A room extension activated for request")

    async def _room_snapshot(self, conversation_id: str) -> tuple[Task, bool]:
        db = self._app.state.db
        conversation = await projections.fetch_conversation(db, conversation_id)
        if conversation is None:
            raise TaskNotFound(conversation_id)
        messages = await projections.fetch_messages(db, conversation_id, limit=1000)
        members = await projections.fetch_room_members(db, conversation_id)
        summary = await projections.fetch_room_summary(db, conversation_id)
        running = False
        task_ids = await projections.fetch_task_ids_for_conversation(
            db, conversation_id
        )
        for task_id in task_ids:
            task = await projections.fetch_task(db, task_id)
            if task is not None and task.status not in TERMINAL_TASK_STATUSES:
                running = True
                break
        room = room_to_task(
            conversation, messages, members, summary, running=running
        )
        return room, running

    async def _room_task(self, conversation_id: str) -> Task:
        room, _ = await self._room_snapshot(conversation_id)
        return room

    async def on_get_task(
        self, params: GetTaskRequest, context: ServerCallContext
    ) -> Task | None:
        self._note_extensions(context)
        try:
            return await self._a2a_task(params.id)
        except TaskNotFound:
            try:
                return await self._room_task(params.id)
            except TaskNotFound:
                return None

    async def on_cancel_task(
        self, params: CancelTaskRequest, context: ServerCallContext
    ) -> Task | None:
        try:
            await cancel_task(
                self._app.state.db,
                self._app.state.event_store,
                self._app.state.remote,
                self._app.state.orchestrator,
                params.id,
            )
        except TaskNotFound:
            return None
        except TaskNotCancelable as exc:
            raise TaskNotCancelableError(str(exc)) from exc
        return await self._a2a_task(params.id)

    async def on_list_tasks(
        self, params: ListTasksRequest, context: ServerCallContext
    ) -> ListTasksResponse:
        raise UnsupportedOperationError("ListTasks is not supported yet")

    async def _submit(self, params: SendMessageRequest) -> Task | Message:
        message = params.message
        text = "\n".join(
            part.text for part in message.parts if part.text
        ).strip()
        if not text:
            raise InvalidParamsError("message text is empty")

        task_id = message.task_id or None
        context_id = message.context_id or None

        if task_id is not None:
            try:
                snapshot = await self._snapshot(task_id)
            except TaskNotFound as exc:
                raise TaskNotFoundError(f"task not found: {task_id}") from exc
            status = snapshot.task.status
            if status is TaskStatus.AWAITING_INPUT:
                interventions = await projections.fetch_interventions(
                    self._app.state.db, task_id, InterventionStatus.PENDING
                )
                if not interventions:
                    raise InvalidParamsError(
                        "task is awaiting input but has no pending intervention"
                    )
                await self._app.state.orchestrator.answer_intervention(
                    interventions[-1].id, text, responder="CEO"
                )
                refreshed = await self._snapshot(task_id)
                for _ in range(40):
                    if refreshed.task.status is not TaskStatus.AWAITING_INPUT:
                        break
                    await asyncio.sleep(0.05)
                    refreshed = await self._snapshot(task_id)
                return snapshot_to_task(
                    refreshed,
                    question=await self._pending_question(refreshed),
                )
            if status in (
                TaskStatus.PENDING,
                TaskStatus.PLANNING,
                TaskStatus.RUNNING,
            ):
                conversation_id = snapshot.task.conversation_id
                if conversation_id is None:
                    conversation_id = await self._app.state.coordinator.create_conversation(
                        title=text[:30]
                    )
                active = next(
                    (
                        node
                        for node in snapshot.nodes
                        if node.status in (NodeStatus.DISPATCHED, NodeStatus.WORKING)
                    ),
                    None,
                )
                row = await post_message(
                    self._app.state.db,
                    self._app.state.event_store,
                    conversation_id=conversation_id,
                    role="user",
                    sender="CEO",
                    text=text,
                    task_id=task_id,
                    queued_for_node_id=active.id if active is not None else None,
                )
                await self._app.state.coordinator.arbitrate_message(
                    snapshot.task, row
                )
                return await self._a2a_task(task_id)
            context_id = snapshot.task.conversation_id or context_id

        if context_id is not None:
            conversation = await projections.fetch_conversation(
                self._app.state.db, context_id
            )
            if conversation is None:
                context_id = None
        if context_id is None:
            context_id = await self._app.state.coordinator.create_conversation(
                title=text[:30]
            )
        try:
            result = await self._app.state.coordinator.handle_human_message(
                context_id, text=text, mentions=[]
            )
        except Exception as exc:  # noqa: BLE001 - 统一映射为 A2A 内部错误
            logger.exception("handle_human_message failed")
            raise InternalError(str(exc)) from exc
        if result.task_id is None:
            return room_message_to_a2a(result.message)
        return await self._a2a_task(result.task_id)

    async def on_message_send(
        self, params: SendMessageRequest, context: ServerCallContext
    ) -> Task | Message:
        self._note_extensions(context)
        return await self._submit(params)

    async def _enrich(self, event: Event) -> Event:
        payload = dict(event.payload)
        if event.type in (
            EventType.PLAN_EXTENDED,
            EventType.PLAN_SUPERSEDED,
        ):
            plan = await projections.fetch_current_plan(
                self._app.state.db, event.task_id
            )
            if plan is not None:
                payload["dag"] = plan.dag
                payload["version"] = plan.version
                payload["rationale"] = plan.rationale
        elif event.type is EventType.NODE_STATE_CHANGED:
            node = await projections.fetch_node(
                self._app.state.db, payload.get("node_id", "")
            )
            if node is not None:
                payload.setdefault("node_name", node.name)
                payload.setdefault("agent_name", node.agent_name)
                payload.setdefault("attempt", node.attempt)
        if payload == event.payload:
            return event
        return event.model_copy(update={"payload": payload})

    async def _stream_task(self, task_id: str):
        bus = self._app.state.event_bus
        snapshot = await self._snapshot(task_id)
        question = await self._pending_question(snapshot)
        mapper = TaskStreamMapper(snapshot)
        seen = snapshot.last_seq
        subscription = bus.subscribe(task_id)
        try:
            yield snapshot_to_task(snapshot, question=question)
            if snapshot.task.status in (
                TaskStatus.COMPLETED,
                TaskStatus.FAILED,
                TaskStatus.CANCELED,
            ):
                return
            for event in await self._app.state.event_store.replay(task_id, seen):
                seen = event.seq
                for response in mapper.map_event(await self._enrich(event)):
                    yield _unwrap_stream_response(response)
                if mapper.terminal:
                    return
            while True:
                try:
                    event = await subscription.get()
                except SubscriptionClosed:
                    break
                if event.seq <= seen:
                    continue
                seen = event.seq
                for response in mapper.map_event(await self._enrich(event)):
                    yield _unwrap_stream_response(response)
                if mapper.terminal:
                    return
        finally:
            subscription.close()
            bus.unsubscribe(subscription)

    async def on_message_send_stream(
        self,
        params: SendMessageRequest,
        context: ServerCallContext,
    ) -> AsyncGenerator[
        Task | Message | TaskStatusUpdateEvent | TaskArtifactUpdateEvent, None
    ]:
        result = await self._submit(params)
        if isinstance(result, Message):
            yield result
            return
        async for response in self._stream_task(result.id):
            yield response

    async def on_subscribe_to_task(
        self,
        params: SubscribeToTaskRequest,
        context: ServerCallContext,
    ) -> AsyncGenerator[
        Task | Message | TaskStatusUpdateEvent | TaskArtifactUpdateEvent, None
    ]:
        try:
            await self._snapshot(params.id)
        except TaskNotFound as exc:
            raise TaskNotFoundError(f"task not found: {params.id}") from exc
        async for response in self._stream_task(params.id):
            yield response

    async def on_get_extended_agent_card(
        self, params: Any, context: ServerCallContext
    ):
        raise UnsupportedOperationError("extended agent card is not supported")

    async def on_create_task_push_notification_config(
        self, params: Any, context: ServerCallContext
    ):
        raise UnsupportedOperationError("push notifications are not supported")

    async def on_get_task_push_notification_config(
        self, params: Any, context: ServerCallContext
    ):
        raise UnsupportedOperationError("push notifications are not supported")

    async def on_list_task_push_notification_configs(
        self, params: Any, context: ServerCallContext
    ):
        raise UnsupportedOperationError("push notifications are not supported")

    async def on_delete_task_push_notification_config(
        self, params: Any, context: ServerCallContext
    ):
        raise UnsupportedOperationError("push notifications are not supported")
