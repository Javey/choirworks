from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from agent_hub.core.tasks import TaskSnapshot
from agent_hub.models.domain import (
    ConversationSummary,
    RoomMember,
    RoomMessage,
    RoomSummary,
)


class TargetIn(BaseModel):
    agent_name: str
    skill_id: str | None = None
    name: str = "single"
    input: dict[str, Any] | None = None


class CreateTaskIn(BaseModel):
    request: str
    target: TargetIn | None = None
    conversation_id: str | None = None


class CreateTaskOut(BaseModel):
    task_id: str
    plan_id: str | None = None
    node_ids: list[str] = Field(default_factory=list)
    conversation_id: str | None = None


class ConversationDetail(BaseModel):
    conversation: ConversationSummary
    tasks: list[TaskSnapshot] = Field(default_factory=list)


class RegisterAgentIn(BaseModel):
    name: str
    card_url: str = Field(..., description="A2A agent base URL")


class AnswerInterventionIn(BaseModel):
    text: str
    responder: str = "user"


class RollbackIn(BaseModel):
    checkpoint_id: str
    mode: str = Field(default="restart", pattern="^(restart|dry_run)$")


class PostMessageIn(BaseModel):
    text: str = Field(min_length=1)
    mentions: list[str] = Field(default_factory=list)
    quote_id: str | None = None
    interrupt: bool = False


class PostMessageOut(BaseModel):
    message_id: str
    seq: int
    task_id: str | None = None


class RoomMessagesOut(BaseModel):
    messages: list[RoomMessage]
    members: list[RoomMember]
    summary: RoomSummary | None = None
    last_seq: int = 0
