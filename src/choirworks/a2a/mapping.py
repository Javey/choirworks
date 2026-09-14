from __future__ import annotations

from collections import defaultdict
from typing import Any

from a2a.types import (
    Artifact,
    Message,
    Part,
    Role,
    StreamResponse,
    Task,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatusUpdateEvent,
)
from a2a.types import (
    TaskStatus as A2ATaskStatus,
)
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict

from choirworks.core.tasks import TaskSnapshot
from choirworks.models.domain import (
    Conversation,
    Node,
    RoomMember,
    RoomMessage,
    RoomSummary,
)
from choirworks.models.enums import EventType, NodeStatus, TaskStatus
from choirworks.store.event_store import Event

A2A_ROOM_URI = "https://github.com/Javey/choirworks/extensions/room/v1"

TASK_STATE_MAP: dict[TaskStatus, TaskState] = {
    TaskStatus.PENDING: TaskState.TASK_STATE_SUBMITTED,
    TaskStatus.PLANNING: TaskState.TASK_STATE_WORKING,
    TaskStatus.RUNNING: TaskState.TASK_STATE_WORKING,
    TaskStatus.AWAITING_INPUT: TaskState.TASK_STATE_INPUT_REQUIRED,
    TaskStatus.COMPLETED: TaskState.TASK_STATE_COMPLETED,
    TaskStatus.FAILED: TaskState.TASK_STATE_FAILED,
    TaskStatus.CANCELED: TaskState.TASK_STATE_CANCELED,
}

TERMINAL_A2A_STATES = {
    TaskState.TASK_STATE_COMPLETED,
    TaskState.TASK_STATE_FAILED,
    TaskState.TASK_STATE_CANCELED,
}


def struct_value(data: dict[str, Any]) -> struct_pb2.Struct:
    value = struct_pb2.Struct()
    ParseDict(data, value)
    return value


def data_part(data: dict[str, Any]) -> Part:
    part = Part()
    ParseDict({"data": data}, part)
    return part


def artifact_id(node_id: str, inner_id: str) -> str:
    return f"{node_id}:{inner_id}"


def agent_message(text: str, *, message_id: str) -> Message:
    return Message(
        message_id=message_id,
        role=Role.ROLE_AGENT,
        parts=[Part(text=text)],
    )


def _node_metadata(node: Node) -> dict[str, Any]:
    return {
        "id": node.id,
        "name": node.name,
        "status": node.status.value,
        "agent_name": node.agent_name,
        "attempt": node.attempt,
    }


def _collect_artifacts(nodes: list[Node]) -> list[Artifact]:
    artifacts: list[Artifact] = []
    for node in nodes:
        for item in (node.output or {}).get("artifacts", []):
            artifacts.append(
                Artifact(
                    artifact_id=artifact_id(node.id, item["id"]),
                    name=item.get("name") or item["id"],
                    parts=[Part(text=item.get("text", ""))],
                )
            )
    return artifacts


def _status_message(
    snapshot: TaskSnapshot, question: str | None
) -> Message | None:
    task = snapshot.task
    if task.status is TaskStatus.AWAITING_INPUT:
        if question:
            return agent_message(question, message_id=f"{task.id}:question")
        return None
    if task.status is TaskStatus.FAILED:
        error: str | None = None
        for node in snapshot.nodes:
            if node.status is NodeStatus.INVALIDATED:
                continue
            if node.error:
                error = node.error
        if error:
            return agent_message(error, message_id=f"{task.id}:error")
    return None


def snapshot_to_task(
    snapshot: TaskSnapshot, *, question: str | None = None
) -> Task:
    task = snapshot.task
    metadata = {
        "plan_version": task.plan_version,
        "nodes": [_node_metadata(node) for node in snapshot.nodes],
    }
    status = A2ATaskStatus(state=TASK_STATE_MAP[task.status])
    message = _status_message(snapshot, question)
    if message is not None:
        status.message.CopyFrom(message)
    return Task(
        id=task.id,
        context_id=task.conversation_id or "",
        status=status,
        artifacts=_collect_artifacts(snapshot.nodes),
        history=[
            Message(
                message_id=f"{task.id}:request",
                role=Role.ROLE_USER,
                parts=[Part(text=task.request)],
            )
        ],
        metadata=struct_value(metadata),
    )


def room_message_to_a2a(message: Any) -> Message:
    metadata = {
        A2A_ROOM_URI: {
            "kind": "message",
            "sender": message.sender,
            "seq": message.seq,
            "mentions": list(message.mentions),
            "quote_id": message.quote_id,
            "node_id": message.node_id,
            "intervention_id": message.intervention_id,
            "queued_for_node_id": message.queued_for_node_id,
        }
    }
    return Message(
        message_id=message.id,
        context_id=message.conversation_id,
        task_id=message.task_id or message.conversation_id,
        role=Role.ROLE_USER if message.role == "user" else Role.ROLE_AGENT,
        parts=[Part(text=message.text)],
        extensions=[A2A_ROOM_URI],
        metadata=struct_value(metadata),
    )


def room_message_from_event(event: Event) -> RoomMessage:
    payload = event.payload
    return RoomMessage(
        id=payload["message_id"],
        conversation_id=payload.get("conversation_id") or event.conversation_id or "",
        seq=payload["seq"],
        role=payload["role"],
        sender=payload.get("sender"),
        text=payload["text"],
        mentions=payload.get("mentions") or [],
        quote_id=payload.get("quote_id"),
        task_id=payload.get("task_id") or event.task_id,
        node_id=payload.get("node_id"),
        intervention_id=payload.get("intervention_id"),
        queued_for_node_id=payload.get("queued_for_node_id"),
        created_at=event.created_at,
    )


def room_to_task(
    conversation: Conversation,
    messages: list[RoomMessage],
    members: list[RoomMember],
    summary: RoomSummary | None,
    *,
    running: bool,
) -> Task:
    summary_payload: dict[str, Any] = {}
    if summary is not None:
        summary_payload = {
            "covers_seq": summary.covers_seq,
            "updated_at": summary.updated_at.isoformat(),
            "content": summary.summary,
        }
    metadata = {
        A2A_ROOM_URI: {
            "kind": "room",
            "title": conversation.title,
            "members": [
                {
                    "agent_name": member.agent_name,
                    "agent_url": member.agent_url,
                    "reason": member.reason,
                    "joined_at": member.joined_at.isoformat(),
                }
                for member in members
            ],
            "summary": summary_payload,
            "message_count": len(messages),
            "last_seq": messages[-1].seq if messages else 0,
        }
    }
    state = (
        TaskState.TASK_STATE_WORKING
        if running
        else TaskState.TASK_STATE_INPUT_REQUIRED
    )
    return Task(
        id=conversation.id,
        context_id=conversation.id,
        status=A2ATaskStatus(state=state),
        history=[room_message_to_a2a(message) for message in messages],
        metadata=struct_value(metadata),
    )


def room_send_options(message: Message) -> dict[str, Any]:
    options: dict[str, Any] = {
        "mentions": [],
        "quote_id": None,
        "interrupt": False,
    }
    field = message.metadata.fields.get(A2A_ROOM_URI)
    if field is None or field.WhichOneof("kind") != "struct_value":
        return options
    fields = field.struct_value.fields
    mentions_value = fields.get("mentions")
    if mentions_value is not None and mentions_value.WhichOneof("kind") == "list_value":
        options["mentions"] = [
            item.string_value
            for item in mentions_value.list_value.values
            if item.WhichOneof("kind") == "string_value"
        ]
    quote_value = fields.get("quote_id")
    if quote_value is not None and quote_value.WhichOneof("kind") == "string_value":
        options["quote_id"] = quote_value.string_value
    interrupt_value = fields.get("interrupt")
    if (
        interrupt_value is not None
        and interrupt_value.WhichOneof("kind") == "bool_value"
    ):
        options["interrupt"] = interrupt_value.bool_value
    return options


_TERMINAL_NODE_VALUES = {
    NodeStatus.COMPLETED.value,
    NodeStatus.FAILED.value,
    NodeStatus.CANCELED.value,
    NodeStatus.INVALIDATED.value,
}

_PLAN_EVENTS = {
    EventType.PLAN_CREATED,
    EventType.PLAN_EXTENDED,
}

_NOTIFICATION_STATES = {
    EventType.NODE_DISPATCH_INTENT,
    EventType.NODE_DISPATCHED,
    EventType.NODE_STATE_CHANGED,
    EventType.NODE_RETRY_SCHEDULED,
    EventType.NODE_INVALIDATED,
    EventType.NODE_CANCEL_SENT,
    EventType.INTERVENTION_FAILED,
    EventType.CHECKPOINT_CREATED,
    EventType.ROLLBACK_PERFORMED,
    EventType.ERROR,
}


class RoomStreamMapper:
    def __init__(self, conversation_id: str, *, running: bool):
        self._conversation_id = conversation_id
        self._running = running

    def _status_update(self, metadata: dict[str, Any]) -> StreamResponse:
        state = (
            TaskState.TASK_STATE_WORKING
            if self._running
            else TaskState.TASK_STATE_INPUT_REQUIRED
        )
        return StreamResponse(
            status_update=TaskStatusUpdateEvent(
                task_id=self._conversation_id,
                context_id=self._conversation_id,
                status=A2ATaskStatus(state=state),
                metadata=struct_value(metadata),
            )
        )

    def map_event(self, event: Event) -> list[StreamResponse]:
        if event.type is EventType.MESSAGE_POSTED:
            message = room_message_from_event(event)
            return [StreamResponse(message=room_message_to_a2a(message))]
        if event.type is EventType.MESSAGE_DELIVERED:
            return [
                self._status_update(
                    {
                        "kind": "message.delivered",
                        "message_id": event.payload.get("message_id"),
                        "node_id": event.payload.get("node_id"),
                    }
                )
            ]
        if event.type is EventType.ROOM_PARTICIPANT_JOINED:
            return [
                self._status_update(
                    {
                        "kind": "room.participant_joined",
                        "agent_name": event.payload.get("agent_name"),
                        "agent_url": event.payload.get("agent_url"),
                        "reason": event.payload.get("reason"),
                    }
                )
            ]
        if event.type is EventType.ROOM_SUMMARY_UPDATED:
            return [
                self._status_update(
                    {
                        "kind": "room.summary_updated",
                        "covers_seq": event.payload.get("covers_seq"),
                        "summary": event.payload.get("summary") or {},
                    }
                )
            ]
        return []


class TaskStreamMapper:
    def __init__(self, snapshot: TaskSnapshot):
        self.terminal = False
        self._task_id = snapshot.task.id
        self._context_id = snapshot.task.conversation_id or ""
        self._state = TASK_STATE_MAP[snapshot.task.status]
        self._last_error: str | None = None
        self._finalized: set[str] = set()
        self._node_artifacts: dict[str, set[str]] = defaultdict(set)
        for node in snapshot.nodes:
            for item in (node.output or {}).get("artifacts", []):
                self._node_artifacts[node.id].add(item["id"])

    def _status_update(
        self,
        state: TaskState | None = None,
        *,
        metadata: dict[str, Any] | None = None,
        message_text: str | None = None,
        message_id: str = "m",
    ) -> StreamResponse:
        status = A2ATaskStatus(state=state or self._state)
        if message_text is not None:
            status.message.CopyFrom(agent_message(message_text, message_id=message_id))
        return StreamResponse(
            status_update=TaskStatusUpdateEvent(
                task_id=self._task_id,
                context_id=self._context_id,
                status=status,
                metadata=struct_value(metadata or {}),
            )
        )

    def _apply_state(self, state: TaskState) -> list[StreamResponse]:
        if state == self._state:
            return []
        self._state = state
        self._last_error = None
        if state in TERMINAL_A2A_STATES:
            self.terminal = True
        return [self._status_update(state)]

    def _last_chunks(self, node_id: str) -> list[StreamResponse]:
        responses: list[StreamResponse] = []
        for inner_id in sorted(self._node_artifacts.get(node_id, set())):
            key = artifact_id(node_id, inner_id)
            if key in self._finalized:
                continue
            self._finalized.add(key)
            responses.append(
                StreamResponse(
                    artifact_update=TaskArtifactUpdateEvent(
                        task_id=self._task_id,
                        context_id=self._context_id,
                        artifact=Artifact(artifact_id=key),
                        append=True,
                        last_chunk=True,
                    )
                )
            )
        return responses

    def map_event(self, event: Event) -> list[StreamResponse]:
        event_type = event.type
        payload = event.payload
        if event_type is EventType.TASK_STATE_CHANGED:
            state = TASK_STATE_MAP[TaskStatus(payload["to"])]
            responses = self._apply_state(state)
            if not responses:
                return []
            responses[0].status_update.metadata.CopyFrom(
                struct_value({"from": payload.get("from"), "to": payload.get("to")})
            )
            return responses
        if event_type is EventType.TASK_COMPLETED:
            return self._apply_state(TaskState.TASK_STATE_COMPLETED)
        if event_type is EventType.TASK_FAILED:
            error = self._last_error
            self._last_error = None
            responses = self._apply_state(TaskState.TASK_STATE_FAILED)
            if responses and error:
                responses[0].status_update.status.message.CopyFrom(
                    agent_message(error, message_id=f"{self._task_id}:error")
                )
            return responses
        if event_type is EventType.NODE_ARTIFACT:
            node_id = payload["node_id"]
            self._node_artifacts[node_id].add(payload["artifact_id"])
            self._finalized.discard(artifact_id(node_id, payload["artifact_id"]))
            return [
                StreamResponse(
                    artifact_update=TaskArtifactUpdateEvent(
                        task_id=self._task_id,
                        context_id=self._context_id,
                        artifact=Artifact(
                            artifact_id=artifact_id(node_id, payload["artifact_id"]),
                            name=payload.get("name") or "",
                            parts=[Part(text=payload.get("text", ""))],
                        ),
                        append=bool(payload.get("append")),
                        last_chunk=False,
                        metadata=struct_value({"node_id": node_id}),
                    )
                )
            ]
        if event_type is EventType.PLAN_SUPERSEDED:
            return [
                self._status_update(
                    metadata={
                        "kind": event_type.value,
                        "plan_id": payload.get("plan_id"),
                        "superseded_by_version": payload.get("superseded_by_version"),
                    }
                )
            ]
        if event_type in _PLAN_EVENTS:
            metadata = {
                "kind": event_type.value,
                "plan_id": payload.get("plan_id"),
                "version": payload.get("version"),
                "rationale": payload.get("rationale"),
            }
            dag = payload.get("dag")
            if dag is None:
                return [self._status_update(metadata=metadata)]
            return [
                StreamResponse(
                    artifact_update=TaskArtifactUpdateEvent(
                        task_id=self._task_id,
                        context_id=self._context_id,
                        artifact=Artifact(
                            artifact_id=f"plan:{payload.get('plan_id')}",
                            name="plan",
                            parts=[data_part(dag)],
                        ),
                        append=False,
                        last_chunk=True,
                        metadata=struct_value(metadata),
                    )
                )
            ]
        if event_type is EventType.INTERVENTION_REQUESTED:
            question = (payload.get("question") or {}).get("text", "")
            self._state = TaskState.TASK_STATE_INPUT_REQUIRED
            return [
                self._status_update(
                    TaskState.TASK_STATE_INPUT_REQUIRED,
                    metadata={
                        "kind": event_type.value,
                        "intervention_id": payload.get("intervention_id"),
                        "node_id": payload.get("node_id"),
                        "policy": payload.get("policy"),
                    },
                    message_text=question,
                    message_id=payload.get("intervention_id") or "intervention",
                )
            ]
        if event_type is EventType.INTERVENTION_RESOLVED:
            answer = (payload.get("answer") or {}).get("text", "")
            metadata = {
                "kind": event_type.value,
                "intervention_id": payload.get("intervention_id"),
                "answer": answer,
            }
            if self._state == TaskState.TASK_STATE_INPUT_REQUIRED:
                self._state = TaskState.TASK_STATE_WORKING
                self._last_error = None
                return [
                    self._status_update(
                        TaskState.TASK_STATE_WORKING,
                        metadata=metadata,
                        message_text=answer or None,
                        message_id=payload.get("intervention_id") or "intervention",
                    )
                ]
            return [self._status_update(metadata=metadata)]
        if event_type in _NOTIFICATION_STATES:
            if event_type is EventType.ERROR:
                self._last_error = payload.get("message")
            if event_type is EventType.CHECKPOINT_CREATED:
                metadata = {
                    "kind": event_type.value,
                    "checkpoint_id": payload.get("checkpoint_id"),
                    "seq": payload.get("seq"),
                    "plan_version": payload.get("plan_version"),
                    "frontier": payload.get("frontier"),
                }
            elif event_type is EventType.ROLLBACK_PERFORMED:
                metadata = {
                    "kind": event_type.value,
                    "reset_node_ids": payload.get("reset_node_ids"),
                }
                if "invalidate_node_ids" in payload:
                    metadata["invalidate_node_ids"] = payload["invalidate_node_ids"]
                if "cancelled_remote_task_ids" in payload:
                    metadata["cancelled_remote_task_ids"] = payload[
                        "cancelled_remote_task_ids"
                    ]
            else:
                metadata = {**payload, "kind": event_type.value}
            responses = [self._status_update(metadata=metadata)]
            if (
                event_type is EventType.NODE_STATE_CHANGED
                and payload.get("to") in _TERMINAL_NODE_VALUES
            ):
                responses.extend(self._last_chunks(payload["node_id"]))
            return responses
        return []
