from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field

from choirworks.models.domain import RoomMessage
from choirworks.store import projections

HEADER_RULES = "需要他人配合时 @姓名 并说明需求；完成后给出结论"


class ContextPackage(BaseModel):
    text: str
    included_message_ids: list[str] = Field(default_factory=list)
    truncated: bool = False


def _estimate(text: str) -> int:
    return max(1, len(text) // 4)


def _render_message(message: RoomMessage, quotes: dict[str, RoomMessage]) -> str:
    label = message.sender or message.role
    prefix = f"#{message.seq} [{label}]"
    if message.quote_id:
        quoted = quotes.get(message.quote_id)
        if quoted is not None:
            snippet = quoted.text[:40]
            prefix += f"（引用 #{quoted.seq} [{quoted.sender or quoted.role}] {snippet}）"
        else:
            prefix += f"（引用 {message.quote_id[:8]}）"
    return f"{prefix} {message.text}"


async def build_agent_context(
    db: Any,
    conversation_id: str,
    agent_name: str,
    instruction: str,
    *,
    budget: int = 8000,
    recent_window: int = 20,
) -> ContextPackage:
    conversation = await projections.fetch_conversation(db, conversation_id)
    title = conversation.title if conversation is not None else conversation_id
    members = await projections.fetch_room_members(db, conversation_id)
    summary = await projections.fetch_room_summary(db, conversation_id)
    messages = await projections.fetch_messages(db, conversation_id, limit=1000)
    quotes = {message.id: message for message in messages}

    relevant: list[RoomMessage] = []
    for message in messages:
        quoted = quotes.get(message.quote_id) if message.quote_id else None
        if (
            agent_name in message.mentions
            or message.sender == agent_name
            or (quoted is not None and quoted.sender == agent_name)
        ):
            relevant.append(message)
    recent = messages[-recent_window:] if recent_window > 0 else []

    members_text = ", ".join(member.agent_name for member in members) or "（暂无）"
    header = (
        f"[系统] 群聊：{title} ｜ 成员：{members_text}\n"
        f"[系统] 规则：{HEADER_RULES}"
    )
    instruction_line = f"[当前任务] {instruction}"
    used = _estimate(header) + _estimate(instruction_line)
    truncated = False

    included: dict[str, RoomMessage] = {}
    for group in (relevant, recent):
        for message in group:
            if message.id in included:
                continue
            cost = _estimate(_render_message(message, quotes))
            if used + cost > budget:
                truncated = True
                continue
            used += cost
            included[message.id] = message

    ordered = sorted(included.values(), key=lambda message: message.seq)
    relevant_ids = {message.id for message in relevant}
    sections: list[str] = [header]
    if summary is not None:
        compact = json.dumps(summary.summary, ensure_ascii=False)
        sections.append(f"[摘要·截至#{summary.covers_seq}] {compact}")
    relevant_lines = [
        _render_message(message, quotes)
        for message in ordered
        if message.id in relevant_ids
    ]
    if relevant_lines:
        sections.append("[与我相关]\n" + "\n".join(relevant_lines))
    recent_lines = [
        _render_message(message, quotes)
        for message in ordered
        if message.id not in relevant_ids
    ]
    if recent_lines:
        sections.append("[最近消息]\n" + "\n".join(recent_lines))
    sections.append(instruction_line)
    return ContextPackage(
        text="\n".join(sections),
        included_message_ids=[message.id for message in ordered],
        truncated=truncated,
    )
