from __future__ import annotations

import asyncio
from typing import Any

from a2a.types import TaskState

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.core.state import assert_node_transition
from agent_hub.models.domain import Node
from agent_hub.models.enums import EventType, NodeStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore

_REMOTE_STATE_MAP: dict[int, NodeStatus] = {
    TaskState.TASK_STATE_SUBMITTED: NodeStatus.DISPATCHED,
    TaskState.TASK_STATE_WORKING: NodeStatus.WORKING,
    TaskState.TASK_STATE_INPUT_REQUIRED: NodeStatus.INPUT_REQUIRED,
    TaskState.TASK_STATE_AUTH_REQUIRED: NodeStatus.INPUT_REQUIRED,
    TaskState.TASK_STATE_COMPLETED: NodeStatus.COMPLETED,
    TaskState.TASK_STATE_FAILED: NodeStatus.FAILED,
    TaskState.TASK_STATE_CANCELED: NodeStatus.CANCELED,
    TaskState.TASK_STATE_REJECTED: NodeStatus.FAILED,
    TaskState.TASK_STATE_UNSPECIFIED: NodeStatus.WORKING,
}


class InvalidNodeState(RuntimeError):
    pass


def _join_text(parts: Any) -> str:
    return "\n".join(part.text for part in parts if part.HasField("text"))


class NodeDispatcher:
    def __init__(
        self,
        db: Database,
        event_store: EventStore,
        remote: RemoteAgentClient,
        timeout_seconds: float = 600.0,
    ):
        self._db = db
        self._events = event_store
        self._remote = remote
        self._timeout = timeout_seconds

    async def dispatch_node(self, task_id: str, node_id: str) -> Node:
        node = await projections.fetch_node(self._db, node_id)
        if node is None or node.task_id != task_id:
            raise InvalidNodeState(f"node not found in task {task_id}: {node_id}")
        if node.status is NodeStatus.PENDING:
            await self._transition(node, NodeStatus.READY)
        if node.status is not NodeStatus.READY:
            raise InvalidNodeState(f"node {node_id} is {node.status.value}, cannot dispatch")

        attempt = node.attempt + 1
        message_id = f"{task_id}:{node_id}:{attempt}"
        await self._events.append(
            task_id,
            EventType.NODE_DISPATCH_INTENT,
            {"node_id": node_id, "message_id": message_id, "attempt": attempt},
        )

        artifacts: list[dict[str, Any]] = []
        current = NodeStatus.READY
        try:
            async with asyncio.timeout(self._timeout):
                text = str((node.input or {}).get("text", ""))
                async for chunk in self._remote.send_text(
                    node.agent_url or "",
                    text,
                    context_id=task_id,
                    message_id=message_id,
                ):
                    if chunk.HasField("task"):
                        await self._events.append(
                            task_id,
                            EventType.NODE_DISPATCHED,
                            {
                                "node_id": node_id,
                                "a2a_task_id": chunk.task.id,
                                "a2a_context_id": chunk.task.context_id,
                                "message_id": message_id,
                            },
                        )
                        current = NodeStatus.DISPATCHED
                        node.status = NodeStatus.DISPATCHED
                    elif chunk.HasField("status_update"):
                        mapped = _REMOTE_STATE_MAP.get(chunk.status_update.status.state)
                        if mapped is not None and mapped is not current:
                            await self._transition(node, mapped)
                            current = mapped
                    elif chunk.HasField("artifact_update"):
                        artifact = chunk.artifact_update.artifact
                        artifact_text = _join_text(artifact.parts)
                        artifacts.append(
                            {
                                "id": artifact.artifact_id,
                                "name": artifact.name,
                                "text": artifact_text,
                            }
                        )
                        await self._events.append(
                            task_id,
                            EventType.NODE_ARTIFACT,
                            {
                                "node_id": node_id,
                                "artifact_id": artifact.artifact_id,
                                "name": artifact.name,
                                "text": artifact_text,
                            },
                        )
                    elif chunk.HasField("message"):
                        message_text = _join_text(chunk.message.parts)
                        artifacts.append(
                            {"id": "message", "name": "message", "text": message_text}
                        )

            if current is NodeStatus.COMPLETED:
                await self._events.append(
                    task_id,
                    EventType.NODE_OUTPUT,
                    {"node_id": node_id, "output": {"artifacts": artifacts}},
                )
            elif current is NodeStatus.INPUT_REQUIRED:
                pass
            elif current not in (NodeStatus.FAILED, NodeStatus.CANCELED):
                await self._fail(node, "remote stream ended without terminal state")
        except TimeoutError:
            await self._fail(node, f"node timed out after {self._timeout}s")
        except Exception as exc:  # noqa: BLE001 - 记录任意远端异常并标记节点失败
            await self._fail(node, str(exc))

        refreshed = await projections.fetch_node(self._db, node_id)
        assert refreshed is not None
        return refreshed

    async def _transition(self, node: Node, target: NodeStatus) -> None:
        assert_node_transition(node.status, target)
        await self._events.append(
            node.task_id,
            EventType.NODE_STATE_CHANGED,
            {"node_id": node.id, "from": node.status.value, "to": target.value},
        )
        node.status = target

    async def _fail(self, node: Node, message: str) -> None:
        if node.status is not NodeStatus.FAILED:
            await self._transition(node, NodeStatus.FAILED)
        await self._events.append(
            node.task_id,
            EventType.ERROR,
            {"node_id": node.id, "message": message},
        )
