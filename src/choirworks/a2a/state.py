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
    answer_text: str | None = None
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
            "answer_text": self.answer_text,
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
            answer_text=data.get("answer_text"),
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
    kind: str = "question"
    target_node_id: str | None = None
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
            "kind": self.kind,
            "target_node_id": self.target_node_id,
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
            kind=str(data.get("kind", "question")),
            target_node_id=data.get("target_node_id"),
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
    """Conversation state, canonical in the contexts store by context_id.

    Every persist also merges a full ``choirworks.state`` snapshot into the
    active A2A Task; that per-turn snapshot is the rewind checkpoint the
    contexts row is restored from.
    """

    plan_id: str = ""
    plan_version: int = 1
    nodes: dict[str, NodeState] = field(default_factory=dict)
    members: dict[str, Member] = field(default_factory=dict)
    interventions: dict[str, Intervention] = field(default_factory=dict)
    queue: dict[str, list[QueuedMessage]] = field(default_factory=dict)
    derived_count: int = 0
    patch_count: int = 0
    revision_count: int = 0
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
        """True when every task is settled: completed or canceled."""
        active = [n for n in self.nodes.values() if n.status != "invalidated"]
        return bool(active) and all(
            n.status in {"completed", "canceled"} for n in active
        )

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

    def add_cancel_request(
        self, target_node_id: str, question: str
    ) -> Intervention | None:
        for intervention in self.interventions.values():
            if (
                intervention.kind == "confirm_cancel"
                and intervention.status == "pending"
                and intervention.target_node_id == target_node_id
            ):
                return None
        intervention = Intervention(
            id=f"iv{self.next_intervention}",
            node_id=target_node_id,
            question=question,
            kind="confirm_cancel",
            target_node_id=target_node_id,
        )
        self.next_intervention += 1
        self.interventions[intervention.id] = intervention
        return intervention

    def expire_cancel_requests(self, node_id: str) -> list[Intervention]:
        expired = []
        for intervention in self.interventions.values():
            if (
                intervention.kind == "confirm_cancel"
                and intervention.status == "pending"
                and intervention.target_node_id == node_id
            ):
                intervention.status = "expired"
                expired.append(intervention)
        return expired

    def normalize_cancel_requests(self) -> list[Intervention]:
        """Expire confirmations whose target is no longer in flight."""
        expired = []
        for intervention in self.interventions.values():
            if (
                intervention.kind != "confirm_cancel"
                or intervention.status != "pending"
                or intervention.target_node_id is None
            ):
                continue
            node = self.nodes.get(intervention.target_node_id)
            if node is None or node.status not in ACTIVE_NODE_STATUSES:
                intervention.status = "expired"
                expired.append(intervention)
        return expired

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

    # ------------------------------------------------------------------ plan

    def start_new_plan(self, plan_id: str) -> None:
        """Reset plan-scoped state for a new turn, keeping session members."""
        self.plan_id = plan_id
        self.plan_version += 1
        self.nodes = {}
        self.interventions = {}
        self.queue = {}
        self.derived_count = 0
        self.patch_count = 0
        self.revision_count = 0
        self.next_intervention = 1
        self.next_message = 1

    # ------------------------------------------------------- serialization

    def to_json(self) -> str:
        """Full snapshot: nodes, members, interventions and queue."""
        return json.dumps({
            "plan_id": self.plan_id,
            "plan_version": self.plan_version,
            "nodes": [n.to_dict() for n in self.nodes.values()],
            "members": [m.to_dict() for m in self.members.values()],
            "interventions": [iv.to_dict() for iv in self.interventions.values()],
            "queue": {
                node_id: [qm.to_dict() for qm in messages]
                for node_id, messages in self.queue.items()
            },
            "derived_count": self.derived_count,
            "patch_count": self.patch_count,
            "revision_count": self.revision_count,
            "next_intervention": self.next_intervention,
            "next_message": self.next_message,
        }, ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> OrchestrationState:
        """Rebuild state from a full snapshot."""
        data = json.loads(raw)
        state = cls()
        state.plan_id = str(data.get("plan_id", ""))
        state.plan_version = int(data.get("plan_version", 1))
        for n in data.get("nodes", []):
            node = NodeState.from_dict(n)
            state.nodes[node.id] = node
        for m in data.get("members", []):
            member = Member.from_dict(m)
            state.members[member.name] = member
        for iv in data.get("interventions", []):
            intervention = Intervention.from_dict(iv)
            state.interventions[intervention.id] = intervention
        for node_id, messages in (data.get("queue") or {}).items():
            state.queue[str(node_id)] = [
                QueuedMessage.from_dict(message) for message in messages
            ]
        state.derived_count = int(data.get("derived_count", 0))
        state.patch_count = int(data.get("patch_count", 0))
        state.revision_count = int(data.get("revision_count", 0))
        state.next_intervention = int(data.get("next_intervention", 1))
        state.next_message = int(data.get("next_message", 1))
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

    Reads the ``choirworks.state`` JSON turn checkpoint; the canonical
    current state lives in the contexts store.
    """
    if task is None or not task.metadata.fields:
        return None
    raw = task.metadata.fields.get(STATE_JSON_KEY)
    if raw is None or not raw.HasField("string_value") or not raw.string_value:
        return None
    try:
        return OrchestrationState.from_json(raw.string_value)
    except (ValueError, TypeError):
        return None
