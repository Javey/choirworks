from __future__ import annotations

from typing import Any

from a2a.types import (
    Artifact,
    Message,
    Part,
    Role,
    Task,
    TaskState,
)
from a2a.types import (
    TaskStatus as A2ATaskStatus,
)
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict

from choirworks.core.tasks import TaskSnapshot
from choirworks.models.domain import Node
from choirworks.models.enums import NodeStatus, TaskStatus

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
