from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from choirworks.models.enums import InterventionStatus, NodeStatus, TaskStatus


class OrchestrationTask(BaseModel):
    id: str
    status: TaskStatus
    request: str
    policy: dict[str, Any] | None = None
    plan_version: int | None = None
    conversation_id: str | None = None
    created_at: datetime
    updated_at: datetime


class Conversation(BaseModel):
    id: str
    title: str
    created_at: datetime


class ConversationSummary(BaseModel):
    id: str
    title: str
    created_at: datetime
    updated_at: datetime
    task_count: int
    last_status: TaskStatus


class Plan(BaseModel):
    id: str
    task_id: str
    version: int
    dag: dict[str, Any]
    rationale: str | None = None
    created_at: datetime


class Node(BaseModel):
    id: str
    task_id: str
    plan_id: str
    name: str
    agent_url: str | None = None
    agent_name: str | None = None
    skill_id: str | None = None
    deps: list[str] = Field(default_factory=list)
    input: dict[str, Any] | None = None
    status: NodeStatus = NodeStatus.PENDING
    attempt: int = 0
    requires_approval: bool = False
    policy_override: str | None = None
    a2a_task_id: str | None = None
    a2a_context_id: str | None = None
    output: dict[str, Any] | None = None
    error: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None


class Intervention(BaseModel):
    id: str
    task_id: str
    node_id: str | None = None
    assigned_node_id: str | None = None
    assigned_to: str | None = None
    source: str
    policy: str
    question: dict[str, Any]
    answer: dict[str, Any] | None = None
    responder: str | None = None
    status: InterventionStatus = InterventionStatus.PENDING
    deadline_at: datetime | None = None
    created_at: datetime
    resolved_at: datetime | None = None


class Checkpoint(BaseModel):
    id: str
    task_id: str
    seq: int
    plan_version: int
    frontier: list[str]
    artifacts: dict[str, Any]
    created_at: datetime


class AgentRecord(BaseModel):
    id: str
    name: str
    card_url: str
    card: dict[str, Any]
    health: str = "unknown"
    last_seen: datetime | None = None
    created_at: datetime


class RoomMessage(BaseModel):
    id: str
    conversation_id: str
    seq: int
    role: str
    sender: str | None = None
    text: str
    mentions: list[str] = Field(default_factory=list)
    quote_id: str | None = None
    task_id: str | None = None
    node_id: str | None = None
    intervention_id: str | None = None
    queued_for_node_id: str | None = None
    delivered_at: datetime | None = None
    created_at: datetime


class RoomMember(BaseModel):
    conversation_id: str
    agent_name: str
    agent_url: str
    reason: str | None = None
    joined_at: datetime


class RoomSummary(BaseModel):
    conversation_id: str
    covers_seq: int
    summary: dict[str, Any]
    updated_at: datetime
