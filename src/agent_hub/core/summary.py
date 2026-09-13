from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field

from agent_hub.models.domain import RoomSummary
from agent_hub.models.enums import EventType
from agent_hub.store import projections

SUMMARY_TRIGGER = 12
SUMMARY_SYSTEM = "你是多 Agent 工作群的会议纪要员，将群聊记录增量压缩为结构化摘要。"


class SummaryDraft(BaseModel):
    goal: str = ""
    decisions: list[str] = Field(default_factory=list)
    artifacts: list[str] = Field(default_factory=list)
    todos: list[str] = Field(default_factory=list)
    open_questions: list[str] = Field(default_factory=list)


async def maybe_update_summary(
    db: Any,
    events: Any,
    llm: Any,
    conversation_id: str,
    *,
    trigger: int = SUMMARY_TRIGGER,
) -> RoomSummary | None:
    summary = await projections.fetch_room_summary(db, conversation_id)
    covers_seq = summary.covers_seq if summary is not None else 0
    messages = await projections.fetch_messages(
        db, conversation_id, after_seq=covers_seq
    )
    if len(messages) < trigger:
        return None
    previous = (
        json.dumps(summary.summary, ensure_ascii=False)
        if summary is not None
        else "（无）"
    )
    transcript = "\n".join(
        f"#{message.seq} [{message.sender or message.role}] {message.text}"
        for message in messages
    )
    try:
        draft = await llm.structured(
            system=SUMMARY_SYSTEM,
            user=f"已有摘要：{previous}\n\n新增消息：\n{transcript}",
            schema=SummaryDraft,
        )
    except Exception as exc:  # noqa: BLE001 - 摘要失败不阻塞消息流
        await events.append(
            None,
            EventType.ERROR,
            {"message": f"摘要更新失败：{exc}"},
            conversation_id=conversation_id,
        )
        return None
    await events.append(
        None,
        EventType.ROOM_SUMMARY_UPDATED,
        {
            "conversation_id": conversation_id,
            "covers_seq": messages[-1].seq,
            "summary": draft.model_dump(),
        },
        conversation_id=conversation_id,
    )
    return await projections.fetch_room_summary(db, conversation_id)
