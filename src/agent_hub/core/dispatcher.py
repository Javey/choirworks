from __future__ import annotations

import asyncio
from typing import Any

from a2a.types import TaskState

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.core.context import ContextPackage, build_agent_context
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
        package = await self._context_for(node)
        intent_payload: dict[str, Any] = {
            "node_id": node_id,
            "message_id": message_id,
            "attempt": attempt,
        }
        if package is not None:
            intent_payload["context_included"] = package.included_message_ids
        await self._events.append(
            task_id,
            EventType.NODE_DISPATCH_INTENT,
            intent_payload,
        )

        artifacts: list[dict[str, Any]] = []
        current = NodeStatus.READY
        try:
            text = (
                package.text
                if package is not None
                else str((node.input or {}).get("text", ""))
            )
            async with asyncio.timeout(self._timeout):
                current = await self._consume(
                    node,
                    self._remote.send_text(
                        node.agent_url or "",
                        text,
                        context_id=task_id,
                        message_id=message_id,
                    ),
                    artifacts,
                    announce_dispatched=True,
                    message_id=message_id,
                )
            await self._handle_stream_end(node, current, artifacts)
        except TimeoutError:
            await self._fail(node, f"node timed out after {self._timeout}s")
        except Exception as exc:  # noqa: BLE001 - 记录任意远端异常并标记节点失败
            await self._fail(node, str(exc))

        refreshed = await projections.fetch_node(self._db, node_id)
        assert refreshed is not None
        return refreshed

    async def continue_node(self, task_id: str, node_id: str, text: str) -> Node:
        node = await projections.fetch_node(self._db, node_id)
        if node is None or node.task_id != task_id:
            raise InvalidNodeState(f"node not found in task {task_id}: {node_id}")
        if node.status is not NodeStatus.INPUT_REQUIRED:
            raise InvalidNodeState(
                f"node {node_id} is {node.status.value}, cannot continue"
            )
        if not node.a2a_task_id:
            raise InvalidNodeState(f"node {node_id} has no remote task id")

        await self._transition(node, NodeStatus.WORKING)
        package = await self._context_for(node)
        continue_text = package.text if package is not None else text
        artifacts: list[dict[str, Any]] = []
        current = NodeStatus.WORKING
        try:
            async with asyncio.timeout(self._timeout):
                current = await self._consume(
                    node,
                    self._remote.send_text(
                        node.agent_url or "",
                        continue_text,
                        task_id=node.a2a_task_id,
                        context_id=node.a2a_context_id,
                        message_id=f"{task_id}:{node_id}:continue:{node.attempt}",
                    ),
                    artifacts,
                    announce_dispatched=False,
                )
            await self._handle_stream_end(node, current, artifacts)
        except TimeoutError:
            await self._fail(node, f"node timed out after {self._timeout}s")
        except Exception as exc:  # noqa: BLE001
            await self._fail(node, str(exc))

        refreshed = await projections.fetch_node(self._db, node_id)
        assert refreshed is not None
        return refreshed

    async def resume_node(self, task_id: str, node_id: str) -> Node:
        node = await projections.fetch_node(self._db, node_id)
        if node is None or node.task_id != task_id:
            raise InvalidNodeState(f"node not found in task {task_id}: {node_id}")
        if node.status not in (NodeStatus.DISPATCHED, NodeStatus.WORKING):
            return node
        if not node.a2a_task_id:
            raise InvalidNodeState(f"node {node_id} has no remote task id")

        artifacts: list[dict[str, Any]] = []
        current = node.status
        try:
            async with asyncio.timeout(self._timeout):
                current = await self._consume(
                    node,
                    self._remote.subscribe_task(
                        node.agent_url or "", node.a2a_task_id
                    ),
                    artifacts,
                    announce_dispatched=False,
                )
            await self._handle_stream_end(node, current, artifacts)
        except TimeoutError:
            await self._fail(node, f"node timed out after {self._timeout}s")
        except Exception as exc:  # noqa: BLE001 - 重挂接失败按节点失败处理
            await self._fail(node, str(exc))

        refreshed = await projections.fetch_node(self._db, node_id)
        assert refreshed is not None
        return refreshed

    async def _context_for(self, node: Node) -> ContextPackage | None:
        task = await projections.fetch_task(self._db, node.task_id)
        if task is None or task.conversation_id is None or not node.agent_name:
            return None
        messages = await projections.fetch_messages(
            self._db, task.conversation_id, limit=1
        )
        if not messages:
            return None
        instruction = str((node.input or {}).get("text", ""))
        return await build_agent_context(
            self._db, task.conversation_id, node.agent_name, instruction
        )

    async def _consume(
        self,
        node: Node,
        chunks: Any,
        artifacts: list[dict[str, Any]],
        *,
        announce_dispatched: bool,
        message_id: str | None = None,
    ) -> NodeStatus:
        current = node.status
        async for chunk in chunks:
            if chunk.HasField("task"):
                if announce_dispatched:
                    await self._events.append(
                        node.task_id,
                        EventType.NODE_DISPATCHED,
                        {
                            "node_id": node.id,
                            "a2a_task_id": chunk.task.id,
                            "a2a_context_id": chunk.task.context_id,
                            "message_id": message_id,
                        },
                    )
                    current = NodeStatus.DISPATCHED
                    node.status = NodeStatus.DISPATCHED
                    node.a2a_task_id = chunk.task.id
                    node.a2a_context_id = chunk.task.context_id
            elif chunk.HasField("status_update"):
                status = chunk.status_update.status
                mapped = _REMOTE_STATE_MAP.get(status.state)
                if mapped is not None and mapped is not current:
                    extra = None
                    if mapped is NodeStatus.INPUT_REQUIRED and status.HasField("message"):
                        extra = {"message": _join_text(status.message.parts)}
                    await self._transition(node, mapped, extra=extra)
                    current = mapped
            elif chunk.HasField("artifact_update"):
                update = chunk.artifact_update
                artifact = update.artifact
                piece = _join_text(artifact.parts)
                append = bool(update.append)
                entry = next(
                    (item for item in artifacts if item["id"] == artifact.artifact_id),
                    None,
                )
                if append and entry is not None:
                    entry["text"] += piece
                    if artifact.name:
                        entry["name"] = artifact.name
                else:
                    merged = {
                        "id": artifact.artifact_id,
                        "name": artifact.name,
                        "text": piece,
                    }
                    if entry is not None:
                        artifacts[artifacts.index(entry)] = merged
                    else:
                        artifacts.append(merged)
                await self._events.append(
                    node.task_id,
                    EventType.NODE_ARTIFACT,
                    {
                        "node_id": node.id,
                        "artifact_id": artifact.artifact_id,
                        "name": artifact.name,
                        "text": piece,
                        "append": append,
                    },
                )
            elif chunk.HasField("message"):
                message_text = _join_text(chunk.message.parts)
                artifacts.append(
                    {"id": "message", "name": "message", "text": message_text}
                )
        return current

    async def _handle_stream_end(
        self, node: Node, current: NodeStatus, artifacts: list[dict[str, Any]]
    ) -> None:
        if current is NodeStatus.COMPLETED:
            await self._events.append(
                node.task_id,
                EventType.NODE_OUTPUT,
                {"node_id": node.id, "output": {"artifacts": artifacts}},
            )
        elif current is NodeStatus.INPUT_REQUIRED:
            return
        elif current not in (NodeStatus.FAILED, NodeStatus.CANCELED):
            await self._fail(node, "remote stream ended without terminal state")

    async def _transition(
        self, node: Node, target: NodeStatus, extra: dict[str, Any] | None = None
    ) -> None:
        assert_node_transition(node.status, target)
        payload: dict[str, Any] = {
            "node_id": node.id,
            "from": node.status.value,
            "to": target.value,
        }
        if extra:
            payload.update(extra)
        await self._events.append(node.task_id, EventType.NODE_STATE_CHANGED, payload)
        node.status = target

    async def _fail(self, node: Node, message: str) -> None:
        if node.status is not NodeStatus.FAILED:
            await self._transition(node, NodeStatus.FAILED)
        await self._events.append(
            node.task_id,
            EventType.ERROR,
            {"node_id": node.id, "message": message},
        )
