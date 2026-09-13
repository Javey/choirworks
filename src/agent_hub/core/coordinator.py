from __future__ import annotations

from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from agent_hub.core.planner import PlanNodeDraft
from agent_hub.core.room import (
    artifact_text,
    extract_mentions,
    post_assistant_message,
    post_message,
)
from agent_hub.core.summary import maybe_update_summary
from agent_hub.core.tasks import TargetSpec
from agent_hub.models.domain import RoomMessage
from agent_hub.models.enums import (
    TERMINAL_NODE_STATUSES,
    TERMINAL_TASK_STATUSES,
    EventType,
    InterventionStatus,
    NodeStatus,
    TaskStatus,
)
from agent_hub.store import projections

MAX_MENTIONS_PER_MESSAGE = 3
MAX_DERIVED_NODES = 5


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
        remote: Any | None = None,
    ):
        self._db = db
        self._events = events
        self._task_service = task_service
        self._orchestrator = orchestrator
        self._registry = registry
        self._llm = llm
        self._remote = remote

    async def join_new_members(
        self, conversation_id: str, names: list[str], *, reason: str = "human_mention"
    ) -> None:
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
                    "reason": reason,
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
            return await self._route_quote(
                conversation_id,
                text=text,
                mentions=mentions,
                quote_id=quote_id,
                interrupt=interrupt,
            )
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

    async def arbitrate_message(self, task: Any, message: RoomMessage) -> int:
        if (
            task.conversation_id is None
            or message.role != "agent"
            or not message.sender
        ):
            return 0
        records = await self._registry.list()
        known = {record.name for record in records}
        mentions = extract_mentions(message.text, known)
        if not mentions:
            return 0
        if len(mentions) > MAX_MENTIONS_PER_MESSAGE:
            await post_assistant_message(
                self._db,
                self._events,
                conversation_id=task.conversation_id,
                text=f"单条消息 @ 数量超限，仅处理前 {MAX_MENTIONS_PER_MESSAGE} 个",
                task_id=task.id,
            )
            mentions = mentions[:MAX_MENTIONS_PER_MESSAGE]

        plan = await projections.fetch_current_plan(self._db, task.id)
        if plan is None:
            return 0
        derived = [
            node for node in plan.dag.get("nodes", []) if node.get("derived")
        ]
        created = 0
        for name in mentions:
            if name == message.sender:
                continue
            await self.join_new_members(
                task.conversation_id, [name], reason="agent_mention"
            )
            existing = next(
                (
                    node
                    for node in derived
                    if node.get("agent_name") == name
                    and (node.get("input") or {}).get("assist_requested_by")
                    == message.sender
                ),
                None,
            )
            if existing is not None:
                await self._merge_into_existing(
                    task, plan.id, existing, message
                )
                continue
            if len(derived) >= MAX_DERIVED_NODES:
                await post_assistant_message(
                    self._db,
                    self._events,
                    conversation_id=task.conversation_id,
                    text=f"协作深度已达上限，忽略 @{name}",
                    task_id=task.id,
                )
                continue
            helper_key = f"a{uuid4().hex[:8]}"
            await self._task_service.extend_plan(
                task.id,
                added_nodes=[
                    PlanNodeDraft(
                        id=helper_key,
                        name=f"协助 · {name}",
                        agent_name=name,
                        input={
                            "text": f"来自 @{message.sender} 的请求：{message.text}",
                            "assist_requested_by": message.sender,
                            "source_message_id": message.id,
                        },
                    )
                ],
                edges=[],
                rationale=f"mention arbitration from {message.sender}",
            )
            await post_assistant_message(
                self._db,
                self._events,
                conversation_id=task.conversation_id,
                text=f"@{message.sender} 请求 @{name} 协助，已加入工作",
                task_id=task.id,
                node_id=f"{plan.id}:{helper_key}",
            )
            derived.append({"id": helper_key, "agent_name": name})
            created += 1
        return created

    async def _merge_into_existing(
        self, task: Any, plan_id: str, dag_node: dict[str, Any], message: RoomMessage
    ) -> None:
        node_id = f"{plan_id}:{dag_node['id']}"
        node = await projections.fetch_node(self._db, node_id)
        target = dag_node.get("agent_name") or dag_node["id"]
        if node is not None and node.status not in TERMINAL_NODE_STATUSES:
            await post_message(
                self._db,
                self._events,
                conversation_id=task.conversation_id,
                role="agent",
                sender=message.sender,
                text=message.text,
                queued_for_node_id=node_id,
                task_id=task.id,
                node_id=message.node_id,
            )
            await post_assistant_message(
                self._db,
                self._events,
                conversation_id=task.conversation_id,
                text=f"已并入 @{target} 的现有工作",
                task_id=task.id,
            )
            return
        await post_assistant_message(
            self._db,
            self._events,
            conversation_id=task.conversation_id,
            text=f"@{target} 已有完成的工作可复用，未重复创建节点",
            task_id=task.id,
        )

    async def _route_quote(
        self,
        conversation_id: str,
        *,
        text: str,
        mentions: list[str],
        quote_id: str | None,
        interrupt: bool,
    ) -> HumanMessageResult:
        if quote_id is None:
            raise ValueError("打断需要引用一条消息")
        quote = await projections.fetch_message(self._db, quote_id)
        if quote is None or quote.conversation_id != conversation_id:
            raise ValueError("被引用的消息不存在")
        await self.join_new_members(conversation_id, mentions)

        if quote.intervention_id:
            intervention = await projections.fetch_intervention(
                self._db, quote.intervention_id
            )
            if (
                intervention is not None
                and intervention.status is InterventionStatus.PENDING
            ):
                posted = await self._orchestrator.answer_intervention(
                    intervention.id, text, responder="CEO", quote_id=quote.id
                )
                if posted is None:
                    posted = await post_message(
                        self._db,
                        self._events,
                        conversation_id=conversation_id,
                        role="user",
                        sender="CEO",
                        text=text,
                        quote_id=quote.id,
                        intervention_id=intervention.id,
                    )
                return HumanMessageResult(
                    message=posted,
                    task_id=intervention.task_id,
                    routed="intervention_answer",
                )

        if quote.node_id:
            node = await projections.fetch_node(self._db, quote.node_id)
            if node is not None and node.status not in TERMINAL_NODE_STATUSES:
                if interrupt:
                    await self._cancel_task_and_signal(node.task_id)
                    task_id = await self._start_followup(conversation_id, quote, text)
                    message = await post_message(
                        self._db,
                        self._events,
                        conversation_id=conversation_id,
                        role="user",
                        sender="CEO",
                        text=text,
                        quote_id=quote.id,
                        task_id=task_id,
                    )
                    await post_assistant_message(
                        self._db,
                        self._events,
                        conversation_id=conversation_id,
                        text=f"已打断 @{quote.sender} 的当前工作，并转交新任务",
                        task_id=task_id,
                    )
                    return HumanMessageResult(
                        message=message, task_id=task_id, routed="interrupted"
                    )
                message = await post_message(
                    self._db,
                    self._events,
                    conversation_id=conversation_id,
                    role="user",
                    sender="CEO",
                    text=text,
                    quote_id=quote.id,
                    task_id=node.task_id,
                    queued_for_node_id=node.id,
                )
                await post_assistant_message(
                    self._db,
                    self._events,
                    conversation_id=conversation_id,
                    text=f"已排队，将在 @{quote.sender} 当前工作结束后投递",
                    task_id=node.task_id,
                )
                return HumanMessageResult(
                    message=message, task_id=node.task_id, routed="queued"
                )

        task_id = await self._start_followup(conversation_id, quote, text)
        message = await post_message(
            self._db,
            self._events,
            conversation_id=conversation_id,
            role="user",
            sender="CEO",
            text=text,
            quote_id=quote.id,
            task_id=task_id,
        )
        await self._maybe_summarize(conversation_id)
        return HumanMessageResult(message=message, task_id=task_id, routed="follow_up")

    async def _start_followup(
        self, conversation_id: str, quote: RoomMessage, text: str
    ) -> str:
        agent_name = quote.sender
        record = (
            await self._registry.get_by_name(agent_name)
            if agent_name and agent_name not in {"CEO", "assistant"}
            else None
        )
        if record is not None:
            await self.join_new_members(
                conversation_id, [agent_name], reason="follow_up"
            )
            created = await self._task_service.create_task(
                text, TargetSpec(agent_name=agent_name), conversation_id=conversation_id
            )
            self._orchestrator.start(created.task_id)
            return created.task_id
        task_id = await self._task_service.create_pending_task(
            text, conversation_id=conversation_id
        )
        self._orchestrator.start(task_id)
        return task_id

    async def _cancel_task_and_signal(self, task_id: str) -> None:
        task = await projections.fetch_task(self._db, task_id)
        if task is None or task.status in TERMINAL_TASK_STATUSES:
            return
        plan = await projections.fetch_current_plan(self._db, task_id)
        if plan is not None and self._remote is not None:
            for node in await projections.fetch_nodes(self._db, task_id, plan.id):
                if node.a2a_task_id and node.status in (
                    NodeStatus.DISPATCHED,
                    NodeStatus.WORKING,
                    NodeStatus.INPUT_REQUIRED,
                ):
                    await self._remote.cancel_task(
                        node.agent_url or "", node.a2a_task_id
                    )
                    await self._events.append(
                        task_id,
                        EventType.NODE_CANCEL_SENT,
                        {"node_id": node.id, "a2a_task_id": node.a2a_task_id},
                    )
        await self._events.append(
            task_id,
            EventType.TASK_STATE_CHANGED,
            {"from": task.status.value, "to": TaskStatus.CANCELED.value},
        )
        await self._orchestrator.stop_task(task_id)

    async def mark_delivered_for_node(self, node_id: str) -> int:
        queued = await projections.fetch_queued_messages(self._db, node_id)
        for message in queued:
            await self._events.append(
                message.task_id,
                EventType.MESSAGE_DELIVERED,
                {"message_id": message.id, "node_id": node_id},
            )
        return len(queued)

    async def deliver_queued_for_terminal(self, task: Any, nodes: list[Any]) -> int:
        if task.conversation_id is None:
            return 0
        forwarded = 0
        for node in nodes:
            if node.status is not NodeStatus.COMPLETED:
                continue
            queued = await projections.fetch_queued_messages(self._db, node.id)
            if not queued or not node.agent_name:
                continue
            text = "\n".join(message.text for message in queued)
            created = await self._task_service.create_task(
                text,
                TargetSpec(agent_name=node.agent_name),
                conversation_id=task.conversation_id,
            )
            await self.mark_delivered_for_node(node.id)
            await post_assistant_message(
                self._db,
                self._events,
                conversation_id=task.conversation_id,
                text=f"已转交 @{node.agent_name} 继续处理",
                task_id=created.task_id,
            )
            self._orchestrator.start(created.task_id)
            forwarded += 1
        return forwarded

    async def reconcile(self) -> int:
        pending = await projections.fetch_undelivered_queued_messages(self._db)
        forwarded = 0
        for message in pending:
            if message.queued_for_node_id is None:
                continue
            node = await projections.fetch_node(self._db, message.queued_for_node_id)
            if node is None or node.status not in TERMINAL_NODE_STATUSES:
                continue
            if not node.agent_name:
                await self.mark_delivered_for_node(node.id)
                continue
            created = await self._task_service.create_task(
                message.text,
                TargetSpec(agent_name=node.agent_name),
                conversation_id=message.conversation_id,
            )
            await self.mark_delivered_for_node(node.id)
            await post_assistant_message(
                self._db,
                self._events,
                conversation_id=message.conversation_id,
                text=f"已转交 @{node.agent_name} 继续处理",
                task_id=created.task_id,
            )
            self._orchestrator.start(created.task_id)
            forwarded += 1
        return forwarded

    async def _maybe_summarize(self, conversation_id: str) -> None:
        if self._llm is None:
            return
        try:
            await maybe_update_summary(
                self._db, self._events, self._llm, conversation_id
            )
        except Exception:  # noqa: BLE001 - 摘要失败不影响消息流
            return
