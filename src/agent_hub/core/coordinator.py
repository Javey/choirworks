from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from agent_hub.core.room import artifact_text, post_assistant_message, post_message
from agent_hub.core.summary import maybe_update_summary
from agent_hub.core.tasks import TargetSpec
from agent_hub.models.domain import RoomMessage
from agent_hub.models.enums import EventType
from agent_hub.store import projections


class HumanMessageResult(BaseModel):
    message: RoomMessage
    task_id: str | None = None
    routed: str = "new_task"


class RoomCoordinator:
    def __init__(
        self,
        db: Any,
        events: Any,
        task_service: Any,
        orchestrator: Any,
        registry: Any,
        llm: Any | None = None,
    ):
        self._db = db
        self._events = events
        self._task_service = task_service
        self._orchestrator = orchestrator
        self._registry = registry
        self._llm = llm

    async def join_new_members(self, conversation_id: str, names: list[str]) -> None:
        members = {
            member.agent_name
            for member in await projections.fetch_room_members(
                self._db, conversation_id
            )
        }
        for name in names:
            if name in members:
                continue
            record = await self._registry.get_by_name(name)
            if record is None:
                continue
            agent_url = self._registry.agent_url(record)
            await self._events.append(
                None,
                EventType.ROOM_PARTICIPANT_JOINED,
                {
                    "agent_name": name,
                    "agent_url": agent_url,
                    "reason": "human_mention",
                },
                conversation_id=conversation_id,
            )
            await post_assistant_message(
                self._db,
                self._events,
                conversation_id=conversation_id,
                text=f"已将 @{name} 加入群聊",
            )
            members.add(name)

    async def handle_human_message(
        self,
        conversation_id: str,
        *,
        text: str,
        mentions: list[str],
        quote_id: str | None = None,
        interrupt: bool = False,
    ) -> HumanMessageResult:
        if quote_id is not None or interrupt:
            raise ValueError("引用与打断路由尚未启用")
        await self.join_new_members(conversation_id, mentions)

        if len(mentions) == 1:
            created = await self._task_service.create_task(
                text,
                TargetSpec(agent_name=mentions[0]),
                conversation_id=conversation_id,
            )
            message = await post_message(
                self._db,
                self._events,
                conversation_id=conversation_id,
                role="user",
                sender="CEO",
                text=text,
                mentions=mentions,
                task_id=created.task_id,
            )
            self._orchestrator.start(created.task_id)
            await self._maybe_summarize(conversation_id)
            return HumanMessageResult(
                message=message, task_id=created.task_id, routed="direct_agent"
            )

        task_id = await self._task_service.create_pending_task(
            text, conversation_id=conversation_id
        )
        message = await post_message(
            self._db,
            self._events,
            conversation_id=conversation_id,
            role="user",
            sender="CEO",
            text=text,
            mentions=mentions,
            task_id=task_id,
        )
        self._orchestrator.start(task_id)
        await self._maybe_summarize(conversation_id)
        return HumanMessageResult(message=message, task_id=task_id, routed="new_task")

    async def announce_plan(self, task: Any, draft: Any) -> RoomMessage | None:
        if task.conversation_id is None:
            return None
        lines = [
            f"- @{node.agent_name} 负责 {node.name}"
            for node in draft.nodes
            if node.agent_name
        ]
        if not lines:
            return None
        return await post_assistant_message(
            self._db,
            self._events,
            conversation_id=task.conversation_id,
            text="任务已拆解：\n" + "\n".join(lines),
            task_id=task.id,
        )

    async def announce_dispatch(self, task: Any, node: Any) -> RoomMessage | None:
        if task.conversation_id is None or not node.agent_name:
            return None
        existing = await projections.fetch_assistant_message_for_node(
            self._db, node.id
        )
        if existing is not None:
            return None
        return await post_assistant_message(
            self._db,
            self._events,
            conversation_id=task.conversation_id,
            text=f"已派发 @{node.agent_name}：{node.name}",
            task_id=task.id,
            node_id=node.id,
        )

    async def announce_completion(self, task: Any, nodes: list[Any]) -> RoomMessage | None:
        if task.conversation_id is None:
            return None
        messages = await projections.fetch_messages(
            self._db, task.conversation_id, limit=1000
        )
        if any(
            message.role == "assistant"
            and message.task_id == task.id
            and message.text.startswith("任务完成")
            for message in messages
        ):
            return None
        lines = [
            f"- {node.agent_name or node.name}：{(artifact_text(node.output) or '（无输出）')[:80]}"
            for node in nodes
            if node.agent_name or node.output
        ]
        return await post_assistant_message(
            self._db,
            self._events,
            conversation_id=task.conversation_id,
            text="任务完成：\n" + "\n".join(lines),
            task_id=task.id,
        )

    async def announce_intervention(
        self, task: Any, node: Any, intervention_id: str, question: str
    ) -> RoomMessage | None:
        if task.conversation_id is None:
            return None
        label = node.agent_name or node.name
        return await post_assistant_message(
            self._db,
            self._events,
            conversation_id=task.conversation_id,
            text=f"@{label} 需要确认：{question}",
            task_id=task.id,
            node_id=node.id,
            intervention_id=intervention_id,
        )

    async def _maybe_summarize(self, conversation_id: str) -> None:
        if self._llm is None:
            return
        try:
            await maybe_update_summary(
                self._db, self._events, self._llm, conversation_id
            )
        except Exception:  # noqa: BLE001 - 摘要失败不影响消息流
            return
