from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from a2a.types.a2a_pb2 import Task
from google.protobuf import struct_pb2
from google.protobuf.json_format import MessageToDict, ParseDict

STATE_JSON_KEY = "choirworks.state"
META_PLAN = "plan"
META_NODES = "nodes"
META_MEMBERS = "members"
META_INTERVENTIONS = "interventions"
META_QUEUE = "queue"

TERMINAL_NODE_STATUSES = {"completed", "failed", "canceled", "invalidated"}
ACTIVE_NODE_STATUSES = {"dispatched", "working"}
PENDING_NODE_STATUSES = {"pending", "ready"}
INPUT_NODE_STATUSES = {"input_required"}
MAX_METADATA_OUTPUT = 2000


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _truncate(text: str | None, limit: int = MAX_METADATA_OUTPUT) -> str | None:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[:limit]


@dataclass
class NodeState:
    id: str
    name: str
    agent_name: str
    agent_url: str
    status: str = "pending"
    attempt: int = 0
    a2a_task_id: str | None = None
    output: str | None = None
    error: str | None = None
    deps: list[str] = field(default_factory=list)
    input_text: str = ""
    derived: bool = False
    policy_override: str | None = None
    question: str | None = None
    source_message_id: str | None = None
    assist_requested_by: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "agent_name": self.agent_name,
            "agent_url": self.agent_url,
            "status": self.status,
            "attempt": self.attempt,
            "a2a_task_id": self.a2a_task_id,
            "output": _truncate(self.output),
            "error": self.error,
            "deps": list(self.deps),
            "input_text": self.input_text,
            "derived": self.derived,
            "policy_override": self.policy_override,
            "question": self.question,
            "source_message_id": self.source_message_id,
            "assist_requested_by": self.assist_requested_by,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NodeState:
        return cls(
            id=str(data.get("id", "")),
            name=str(data.get("name", "")),
            agent_name=str(data.get("agent_name", "")),
            agent_url=str(data.get("agent_url", "")),
            status=str(data.get("status", "pending")),
            attempt=int(data.get("attempt", 0)),
            a2a_task_id=data.get("a2a_task_id"),
            output=data.get("output"),
            error=data.get("error"),
            deps=[str(dep) for dep in data.get("deps", [])],
            input_text=str(data.get("input_text", "")),
            derived=bool(data.get("derived", False)),
            policy_override=data.get("policy_override"),
            question=data.get("question"),
            source_message_id=data.get("source_message_id"),
            assist_requested_by=data.get("assist_requested_by"),
        )


@dataclass
class Member:
    name: str
    url: str
    reason: str
    joined_at: str = field(default_factory=_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "agent_name": self.name,
            "url": self.url,
            "agent_url": self.url,
            "reason": self.reason,
            "joined_at": self.joined_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Member:
        return cls(
            name=str(data.get("name", data.get("agent_name", ""))),
            url=str(data.get("url", data.get("agent_url", ""))),
            reason=str(data.get("reason", "")),
            joined_at=str(data.get("joined_at", _now())),
        )


@dataclass
class Intervention:
    id: str
    node_id: str
    question: str
    status: str = "pending"
    answer: str | None = None
    responder: str | None = None
    created_at: str = field(default_factory=_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "intervention_id": self.id,
            "node_id": self.node_id,
            "question": self.question,
            "status": self.status,
            "answer": self.answer,
            "responder": self.responder,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Intervention:
        return cls(
            id=str(data.get("id", data.get("intervention_id", ""))),
            node_id=str(data.get("node_id", "")),
            question=str(data.get("question", "")),
            status=str(data.get("status", "pending")),
            answer=data.get("answer"),
            responder=data.get("responder"),
            created_at=str(data.get("created_at", _now())),
        )


@dataclass
class QueuedMessage:
    id: str
    text: str
    sender: str
    quote_id: str | None = None
    created_at: str = field(default_factory=_now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "text": self.text,
            "sender": self.sender,
            "quote_id": self.quote_id,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QueuedMessage:
        return cls(
            id=str(data.get("id", "")),
            text=str(data.get("text", "")),
            sender=str(data.get("sender", "user")),
            quote_id=data.get("quote_id"),
            created_at=str(data.get("created_at", _now())),
        )


@dataclass
class OrchestrationState:
    """Persisted in the A2A Task metadata under ``choirworks.state``.

    The A2A Task is the aggregate root; every mutation is emitted as an A2A
    event whose metadata merges into the Task snapshot via DatabaseTaskStore.
    """

    plan_id: str = ""
    plan_version: int = 1
    rationale: str = ""
    nodes: dict[str, NodeState] = field(default_factory=dict)
    members: dict[str, Member] = field(default_factory=dict)
    interventions: dict[str, Intervention] = field(default_factory=dict)
    queue: dict[str, list[QueuedMessage]] = field(default_factory=dict)
    derived_count: int = 0
    next_intervention: int = 1
    next_message: int = 1

    # ------------------------------------------------------------------ plan

    def ready_nodes(self) -> list[NodeState]:
        ready: list[NodeState] = []
        for node in self.nodes.values():
            if node.status not in ("pending", "ready", "resume"):
                continue
            if all(
                dep in self.nodes and self.nodes[dep].status == "completed"
                for dep in node.deps
            ):
                ready.append(node)
        return ready

    def blocked_nodes(self) -> list[NodeState]:
        return [
            node
            for node in self.nodes.values()
            if node.status == "pending"
            and any(
                dep in self.nodes and self.nodes[dep].status in TERMINAL_NODE_STATUSES
                for dep in node.deps
            )
        ]

    def active_nodes(self) -> list[NodeState]:
        return [n for n in self.nodes.values() if n.status in ACTIVE_NODE_STATUSES]

    def input_required_nodes(self) -> list[NodeState]:
        return [n for n in self.nodes.values() if n.status in INPUT_NODE_STATUSES]

    def failed_nodes(self) -> list[NodeState]:
        return [n for n in self.nodes.values() if n.status == "failed"]

    def all_completed(self) -> bool:
        active = [n for n in self.nodes.values() if n.status != "invalidated"]
        return bool(active) and all(n.status == "completed" for n in active)

    def has_failures(self) -> bool:
        return any(n.status == "failed" for n in self.nodes.values())

    def has_pending_work(self) -> bool:
        return any(
            n.status in PENDING_NODE_STATUSES | ACTIVE_NODE_STATUSES | INPUT_NODE_STATUSES
            for n in self.nodes.values()
        )

    def assist_nodes_for(self, agent_name: str, requested_by: str) -> list[NodeState]:
        return [
            node
            for node in self.nodes.values()
            if node.derived
            and node.assist_requested_by == requested_by
            and node.agent_name == agent_name
        ]

    # ------------------------------------------------------------- members

    def add_member(self, name: str, url: str, reason: str) -> bool:
        if name in self.members:
            return False
        self.members[name] = Member(name=name, url=url, reason=reason)
        return True

    # ------------------------------------------------------- interventions

    def pending_interventions(self) -> list[Intervention]:
        return [iv for iv in self.interventions.values() if iv.status == "pending"]

    def pending_intervention_for(self, node_id: str) -> Intervention | None:
        for intervention in self.interventions.values():
            if intervention.node_id == node_id and intervention.status == "pending":
                return intervention
        return None

    def add_intervention(self, node_id: str, question: str) -> Intervention:
        intervention = Intervention(
            id=f"iv{self.next_intervention}",
            node_id=node_id,
            question=question,
        )
        self.next_intervention += 1
        self.interventions[intervention.id] = intervention
        return intervention

    # ------------------------------------------------------------- queue

    def enqueue(self, node_id: str, text: str, sender: str = "user",
                quote_id: str | None = None) -> QueuedMessage:
        message = QueuedMessage(
            id=f"qm{self.next_message}", text=text, sender=sender, quote_id=quote_id
        )
        self.next_message += 1
        self.queue.setdefault(node_id, []).append(message)
        return message

    def take_queued(self, node_id: str) -> list[QueuedMessage]:
        return self.queue.pop(node_id, [])

    # ------------------------------------------------------- serialization

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan": {
                "id": self.plan_id,
                "version": self.plan_version,
                "rationale": self.rationale,
            },
            META_NODES: [node.to_dict() for node in self.nodes.values()],
            META_MEMBERS: [member.to_dict() for member in self.members.values()],
            META_INTERVENTIONS: [
                intervention.to_dict() for intervention in self.interventions.values()
            ],
            META_QUEUE: {
                node_id: [message.to_dict() for message in messages]
                for node_id, messages in self.queue.items()
            },
            "choirworks.runtime": {
                "plan_id": self.plan_id,
                "plan_version": self.plan_version,
                "rationale": self.rationale,
                "derived_count": self.derived_count,
                "next_intervention": self.next_intervention,
                "next_message": self.next_message,
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OrchestrationState:
        plan = data.get("plan") or {}
        runtime = data.get("choirworks.runtime") or {}
        state = cls(
            plan_id=str(plan.get("id", runtime.get("plan_id", ""))),
            plan_version=int(plan.get("version", runtime.get("plan_version", 1))),
            rationale=str(plan.get("rationale", runtime.get("rationale", ""))),
            derived_count=int(runtime.get("derived_count", 0)),
            next_intervention=int(runtime.get("next_intervention", 1)),
            next_message=int(runtime.get("next_message", 1)),
        )
        for node_data in data.get(META_NODES, []) or []:
            node = NodeState.from_dict(node_data)
            state.nodes[node.id] = node
        for member_data in data.get(META_MEMBERS, []) or []:
            member = Member.from_dict(member_data)
            state.members[member.name] = member
        for intervention_data in data.get(META_INTERVENTIONS, []) or []:
            intervention = Intervention.from_dict(intervention_data)
            state.interventions[intervention.id] = intervention
        for node_id, messages in (data.get(META_QUEUE) or {}).items():
            state.queue[node_id] = [
                QueuedMessage.from_dict(message) for message in messages
            ]
        return state

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> OrchestrationState:
        return cls.from_dict(json.loads(raw))

    def metadata(self) -> struct_pb2.Struct:
        struct = struct_pb2.Struct()
        ParseDict(_strip_none(self.to_dict()), struct)
        # Keep a lossless JSON copy for restart recovery (plan ids etc.).
        struct.fields[STATE_JSON_KEY].string_value = self.to_json()
        return struct


def _strip_none(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_none(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list):
        return [_strip_none(item) for item in value]
    return value


def load_state(task: Task | None) -> OrchestrationState | None:
    """Rebuild orchestration state from a persisted A2A Task."""
    if task is None:
        return None
    if not task.metadata.fields:
        return None
    raw = task.metadata.fields.get(STATE_JSON_KEY)
    if raw is not None and raw.HasField("string_value") and raw.string_value:
        try:
            return OrchestrationState.from_json(raw.string_value)
        except (ValueError, TypeError):
            return None
    data = MessageToDict(task.metadata, preserving_proto_field_name=True)
    if not data.get(META_NODES):
        return None
    try:
        return OrchestrationState.from_dict(data)
    except (ValueError, TypeError):
        return None
