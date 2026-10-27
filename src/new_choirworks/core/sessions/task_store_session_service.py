from __future__ import annotations

from typing import override

from a2a.server.context import ServerCallContext
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import (
    Artifact,
    ListTasksRequest,
    Message,
    Role,
    Task,
    TaskArtifactUpdateEvent,
)

from new_choirworks.core.events.event import A2AEvent, meta
from new_choirworks.core.sessions.base_session_service import BaseSessionService
from new_choirworks.core.sessions.session import Session

_PAGE_SIZE = 100


class TaskStoreSessionService(BaseSessionService):
    """从 A2A TaskStore 读取并重建 Session。

    持久化由 A2A SDK 的 TaskManager 负责；本服务只读。
    回放以协议 Task 的 ``artifacts`` 数组为唯一有序事件通道：
    用户消息也投影为 artifact（``author="user"``）。
    """

    _task_store: TaskStore
    _context: ServerCallContext

    def __init__(
        self,
        task_store: TaskStore,
        context: ServerCallContext | None = None,
    ) -> None:
        self._task_store = task_store
        self._context = context or ServerCallContext()

    @override
    async def get_session(self, session_id: str) -> Session | None:
        tasks = await self._list_tasks(session_id)
        if not tasks:
            return None
        session = Session(id=session_id)
        for task in tasks:
            for artifact in task.artifacts:
                _ = self._commit_event_to_session(session, _event_from_artifact(task, artifact))
        return session

    @override
    async def append_event(self, session: Session, event: A2AEvent) -> A2AEvent:
        # 持久化由 SDK/TaskManager 负责；这里只维护内存 Session。
        return await super().append_event(session, event)

    async def _list_tasks(self, context_id: str) -> list[Task]:
        tasks: list[Task] = []
        page_token = ""
        while True:
            params = ListTasksRequest(page_size=_PAGE_SIZE)
            params.context_id = context_id
            if page_token:
                params.page_token = page_token
            page = await self._task_store.list(params, self._context)
            tasks.extend(page.tasks)
            page_token = page.next_page_token
            if not page_token:
                break
        tasks.reverse()  # SDK 返回 newest first；回放要 oldest first
        return tasks


def _event_from_artifact(task: Task, artifact: Artifact) -> A2AEvent:
    event = TaskArtifactUpdateEvent(
        task_id=task.id,
        context_id=task.context_id,
        artifact=artifact,
        append=False,
        last_chunk=True,
    )
    if meta(event).author == "user":
        message = Message(
            task_id=task.id,
            context_id=task.context_id,
            role=Role.ROLE_USER,
        )
        message.parts.extend(artifact.parts)
        message.metadata.CopyFrom(artifact.metadata)
        return message
    return event
