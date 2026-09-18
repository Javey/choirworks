from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from a2a.types.a2a_pb2 import Task

STATE_JSON_KEY = "choirworks.state"

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

    def to_minimal_json(self) -> str:
        """Minimal snapshot for crash recovery: only node DAG + members."""
        return json.dumps({
            "nodes": [
                {
                    "id": n.id,
                    "name": n.name,
                    "agent_name": n.agent_name,
                    "agent_url": n.agent_url,
                    "status": n.status,
                    "deps": list(n.deps),
                    "input_text": n.input_text,
                    "a2a_task_id": n.a2a_task_id,
                    "attempt": n.attempt,
                    "derived": n.derived,
                    "assist_requested_by": n.assist_requested_by,
                    "question": n.question,
                }
                for n in self.nodes.values()
            ],
            "members": [
                {"name": m.name, "url": m.url, "reason": m.reason}
                for m in self.members.values()
            ],
        }, ensure_ascii=False)

    @classmethod
    def from_minimal_json(cls, raw: str) -> OrchestrationState:
        data = json.loads(raw)
        state = cls()
        for n in data.get("nodes", []):
            node = NodeState(
                id=str(n["id"]),
                name=str(n.get("name", "")),
                agent_name=str(n.get("agent_name", "")),
                agent_url=str(n.get("agent_url", "")),
                status=str(n.get("status", "pending")),
                deps=[str(d) for d in n.get("deps", [])],
                input_text=str(n.get("input_text", "")),
                a2a_task_id=n.get("a2a_task_id"),
                attempt=int(n.get("attempt", 0)),
                derived=bool(n.get("derived", False)),
                assist_requested_by=n.get("assist_requested_by"),
                question=n.get("question"),
            )
            state.nodes[node.id] = node
        for m in data.get("members", []):
            member = Member(
                name=str(m["name"]),
                url=str(m.get("url", "")),
                reason=str(m.get("reason", "")),
            )
            state.members[member.name] = member
        state.derived_count = sum(1 for n in state.nodes.values() if n.derived)
        return state


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _truncate(text: str | None, limit: int = MAX_METADATA_OUTPUT) -> str | None:
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[:limit]


def load_state(task: Task | None) -> OrchestrationState | None:
    """Rebuild orchestration state from a persisted A2A Task.

    Reads only the ``choirworks.state`` JSON string field — a minimal
    snapshot of the node DAG + members written at key state transitions.
    """
    if task is None or not task.metadata.fields:
        return None
    raw = task.metadata.fields.get(STATE_JSON_KEY)
    if raw is None or not raw.HasField("string_value") or not raw.string_value:
        return None
    try:
        return OrchestrationState.from_minimal_json(raw.string_value)
    except (ValueError, TypeError):
        return None
