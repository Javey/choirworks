from __future__ import annotations

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
    SubscribeToTaskRequest,
    Task,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)
from a2a.utils.errors import (
    TaskNotCancelableError,
    UnsupportedOperationError,
)
from fastapi import FastAPI

from choirworks.a2a.mapping import snapshot_to_task
from choirworks.core.cancel import TaskNotCancelable, cancel_task
from choirworks.core.tasks import TaskNotFound, TaskSnapshot
from choirworks.models.enums import InterventionStatus, TaskStatus
from choirworks.store import projections


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

    async def on_get_task(
        self, params: GetTaskRequest, context: ServerCallContext
    ) -> Task | None:
        try:
            return await self._a2a_task(params.id)
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

    async def on_message_send(
        self, params: SendMessageRequest, context: ServerCallContext
    ) -> Task | Message:
        raise UnsupportedOperationError("SendMessage not implemented yet")

    async def on_message_send_stream(
        self,
        params: SendMessageRequest,
        context: ServerCallContext,
    ) -> AsyncGenerator[
        Task | Message | TaskStatusUpdateEvent | TaskArtifactUpdateEvent, None
    ]:
        raise UnsupportedOperationError("SendStreamingMessage not implemented yet")
        yield

    async def on_subscribe_to_task(
        self,
        params: SubscribeToTaskRequest,
        context: ServerCallContext,
    ) -> AsyncGenerator[
        Task | Message | TaskStatusUpdateEvent | TaskArtifactUpdateEvent, None
    ]:
        raise UnsupportedOperationError("SubscribeToTask not implemented yet")
        yield

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
