from __future__ import annotations

import asyncio
import re
from typing import Any
from uuid import uuid4

from agent_hub.models.domain import RoomMessage
from agent_hub.models.enums import EventType
from agent_hub.store import projections

_MENTION_RE = re.compile(r"@([A-Za-z0-9_\-]+)")

_seq_locks: dict[str, asyncio.Lock] = {}


def _lock_for(conversation_id: str) -> asyncio.Lock:
    lock = _seq_locks.get(conversation_id)
    if lock is None:
        lock = asyncio.Lock()
        _seq_locks[conversation_id] = lock
    return lock


async def post_message(
    db: Any,
    events: Any,
    *,
    conversation_id: str,
    role: str,
    sender: str | None,
    text: str,
    mentions: list[str] | None = None,
    quote_id: str | None = None,
    task_id: str | None = None,
    node_id: str | None = None,
    intervention_id: str | None = None,
    queued_for_node_id: str | None = None,
) -> RoomMessage:
    async with _lock_for(conversation_id):
        seq = await projections.next_message_seq(db, conversation_id) + 1
        message_id = uuid4().hex
        await events.append(
            task_id,
            EventType.MESSAGE_POSTED,
            {
                "message_id": message_id,
                "conversation_id": conversation_id,
                "seq": seq,
                "role": role,
                "sender": sender,
                "text": text,
                "mentions": mentions or [],
                "quote_id": quote_id,
                "task_id": task_id,
                "node_id": node_id,
                "intervention_id": intervention_id,
                "queued_for_node_id": queued_for_node_id,
            },
            conversation_id=conversation_id,
        )
        message = await projections.fetch_message(db, message_id)
        assert message is not None
        return message


def artifact_text(output: dict[str, Any] | None) -> str:
    if not output:
        return ""
    parts = [
        artifact.get("text", "")
        for artifact in output.get("artifacts", [])
        if isinstance(artifact, dict)
    ]
    return " ".join(part for part in parts if part).strip()


async def post_agent_messages(
    db: Any, events: Any, task: Any, nodes: list[Any]
) -> list[RoomMessage]:
    from agent_hub.models.enums import NodeStatus

    created: list[RoomMessage] = []
    if task.conversation_id is None:
        return created
    for node in nodes:
        if node.status is not NodeStatus.COMPLETED or not node.agent_name:
            continue
        existing = await projections.fetch_messages_for_node(db, node.id)
        if any(message.role == "agent" for message in existing):
            continue
        message = await post_message(
            db,
            events,
            conversation_id=task.conversation_id,
            role="agent",
            sender=node.agent_name,
            text=artifact_text(node.output) or "已完成",
            node_id=node.id,
            task_id=task.id,
        )
        created.append(message)
    return created


def extract_mentions(text: str, known_names: set[str]) -> list[str]:
    found: list[str] = []
    for name in _MENTION_RE.findall(text):
        if name in known_names and name not in found:
            found.append(name)
    return found


async def post_assistant_message(
    db: Any,
    events: Any,
    *,
    conversation_id: str,
    text: str,
    task_id: str | None = None,
    node_id: str | None = None,
    intervention_id: str | None = None,
) -> RoomMessage:
    return await post_message(
        db,
        events,
        conversation_id=conversation_id,
        role="assistant",
        sender="assistant",
        text=text,
        task_id=task_id,
        node_id=node_id,
        intervention_id=intervention_id,
    )
