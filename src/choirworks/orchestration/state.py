from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TypedDict, cast

from a2a.types.a2a_pb2 import Task

from choirworks.core.util import now_iso, truncate

STATE_JSON_KEY = "choirworks.state"


class NodeStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    SUBMITTED = "submitted"
    WORKING = "working"
    INPUT_REQUIRED = "input_required"
    RECOVER = "recover"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    INVALIDATED = "invalidated"


class InterventionStatus(StrEnum):
    PENDING = "pending"
    RESOLVED = "resolved"
    EXPIRED = "expired"


class InterventionKind(StrEnum):
    QUESTION = "question"
    CONFIRM_CANCEL = "confirm_cancel"


class QuestionType(StrEnum):
    INPUT = "input"
    SELECT = "select"
    CONFIRM = "confirm"


TERMINAL_NODE_STATUSES = {
    NodeStatus.COMPLETED,
    NodeStatus.FAILED,
    NodeStatus.CANCELED,
    NodeStatus.INVALIDATED,
}
ACTIVE_NODE_STATUSES = {NodeStatus.SUBMITTED, NodeStatus.WORKING}
PENDING_NODE_STATUSES = {NodeStatus.PENDING, NodeStatus.READY, NodeStatus.RECOVER}
INPUT_NODE_STATUSES = {NodeStatus.INPUT_REQUIRED}
MAX_METADATA_OUTPUT = 2000


class NodeDict(TypedDict):
    """Serialized :class:`NodeState` (persisted snapshot / full payload)."""

    id: str
    name: str
    agent_name: str
    agent_url: str
    status: str
    attempt: int
    a2a_task_id: str | None
    output: str | None
    error: str | None
    deps: list[str]
    input_text: str
    derived: bool
    question: str | None
    answer_text: str | None
    source_message_id: str | None
    assist_requested_by: str | None


@dataclass
class NodeState:
    id: str
    name: str
    agent_name: str
    agent_url: str
    status: NodeStatus = NodeStatus.PENDING
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

    def to_dict(self) -> NodeDict:
        return {
            "id": self.id,
            "name": self.name,
            "agent_name": self.agent_name,
            "agent_url": self.agent_url,
            "status": self.status,
            "attempt": self.attempt,
            "a2a_task_id": self.a2a_task_id,
            "output": truncate(self.output),
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
            status=NodeStatus(data.get("status", "pending")),
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


class MemberDelta(TypedDict):
    """Wire-format delta for a room member joining event."""

    agent_name: str
    agent_url: str
    reason: str


class NodeDelta(TypedDict, total=False):
    """Wire-format partial update for one node in a ``state_delta`` event."""

    status: str
    error: str | None
    a2a_task_id: str | None
    question: str
    agent_name: str
    output: str
    name: str
    input_text: str


class InterventionDelta(TypedDict, total=False):
    """Wire-format partial update for one intervention in a state delta."""

    status: str
    node_id: str
    kind: str
    question: str
    responder: str
    answer: str | list[str] | bool
    question_type: str
    options: list[str]
    multi: bool
    requester: str


class MemberDict(TypedDict):
    """Serialized :class:`Member`."""

    name: str
    agent_name: str
    url: str
    agent_url: str
    reason: str
    joined_at: str


@dataclass
class Member:
    name: str
    url: str
    reason: str
    joined_at: str = field(default_factory=now_iso)

    def to_dict(self) -> MemberDict:
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
            joined_at=str(data.get("joined_at", now_iso())),
        )


class InterventionDict(TypedDict):
    """Serialized :class:`Intervention`."""

    id: str
    intervention_id: str
    node_id: str
    question: str
    status: str
    answer: str | list[str] | bool | None
    responder: str | None
    kind: str
    question_type: str
    options: list[str]
    multi: bool
    requester: str
    target_node_id: str | None
    created_at: str


def _answer_from_json(value: object) -> str | list[str] | bool | None:
    """Validate a persisted answer value (str / list[str] / bool / None)."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return [str(item) for item in value]
    raise ValueError(f"invalid intervention answer: {value!r}")


@dataclass
class Intervention:
    id: str
    node_id: str
    question: str
    status: InterventionStatus = InterventionStatus.PENDING
    answer: str | list[str] | bool | None = None
    responder: str | None = None
    kind: InterventionKind = InterventionKind.QUESTION
    question_type: QuestionType = QuestionType.INPUT
    options: list[str] = field(default_factory=list)
    multi: bool = False
    requester: str = ""
    target_node_id: str | None = None
    created_at: str = field(default_factory=now_iso)

    def to_dict(self) -> InterventionDict:
        return {
            "id": self.id,
            "intervention_id": self.id,
            "node_id": self.node_id,
            "question": self.question,
            "status": self.status,
            "answer": self.answer,
            "responder": self.responder,
            "kind": self.kind,
            "question_type": self.question_type,
            "options": list(self.options),
            "multi": self.multi,
            "requester": self.requester,
            "target_node_id": self.target_node_id,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Intervention:
        raw_options = data.get("options", [])
        return cls(
            id=str(data.get("id", data.get("intervention_id", ""))),
            node_id=str(data.get("node_id", "")),
            question=str(data.get("question", "")),
            status=InterventionStatus(data.get("status", "pending")),
            answer=_answer_from_json(data.get("answer")),
            responder=data.get("responder"),
            kind=InterventionKind(data.get("kind", InterventionKind.QUESTION)),
            question_type=QuestionType(data.get("question_type", QuestionType.INPUT)),
            options=[str(item) for item in raw_options] if isinstance(raw_options, list) else [],
            multi=bool(data.get("multi", False)),
            requester=str(data.get("requester", "")),
            target_node_id=data.get("target_node_id"),
            created_at=str(data.get("created_at", now_iso())),
        )


class QueuedMessageDict(TypedDict):
    """Serialized :class:`QueuedMessage`."""

    id: str
    text: str
    sender: str
    quote_id: str | None
    created_at: str


@dataclass
class QueuedMessage:
    id: str
    text: str
    sender: str
    quote_id: str | None = None
    created_at: str = field(default_factory=now_iso)

    def to_dict(self) -> QueuedMessageDict:
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
            created_at=str(data.get("created_at", now_iso())),
        )


@dataclass
class OrchestrationState:
    """Conversation state, canonical in the contexts store by context_id.

    Every persist also merges a full ``choirworks.state`` snapshot into the
    active A2A Task; that per-turn snapshot is the rewind checkpoint the
    contexts row is restored from.

    Data-only: queries and transitions live as module-level functions below.
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
    next_message: int = 1


# --------------------------------------------------------------------- queries


def ready_nodes(state: OrchestrationState) -> list[NodeState]:
    ready: list[NodeState] = []
    for node in state.nodes.values():
        if node.status not in PENDING_NODE_STATUSES:
            continue
        if all(
            dep in state.nodes and state.nodes[dep].status == NodeStatus.COMPLETED
            for dep in node.deps
        ):
            ready.append(node)
    return ready


def blocked_nodes(state: OrchestrationState) -> list[NodeState]:
    return [
        node
        for node in state.nodes.values()
        if node.status == NodeStatus.PENDING
        and any(
            dep in state.nodes and state.nodes[dep].status in TERMINAL_NODE_STATUSES
            for dep in node.deps
        )
    ]


def active_nodes(state: OrchestrationState) -> list[NodeState]:
    return [n for n in state.nodes.values() if n.status in ACTIVE_NODE_STATUSES]


def input_required_nodes(state: OrchestrationState) -> list[NodeState]:
    return [n for n in state.nodes.values() if n.status in INPUT_NODE_STATUSES]


def failed_nodes(state: OrchestrationState) -> list[NodeState]:
    return [n for n in state.nodes.values() if n.status == NodeStatus.FAILED]


def all_completed(state: OrchestrationState) -> bool:
    """True when every task is settled: completed or canceled."""
    active = [n for n in state.nodes.values() if n.status != NodeStatus.INVALIDATED]
    return bool(active) and all(
        n.status in {NodeStatus.COMPLETED, NodeStatus.CANCELED} for n in active
    )


def has_failures(state: OrchestrationState) -> bool:
    return any(n.status == NodeStatus.FAILED for n in state.nodes.values())


def has_pending_work(state: OrchestrationState) -> bool:
    return any(
        n.status in PENDING_NODE_STATUSES | ACTIVE_NODE_STATUSES | INPUT_NODE_STATUSES
        for n in state.nodes.values()
    )


def assist_nodes_for(
    state: OrchestrationState, agent_name: str, requested_by: str
) -> list[NodeState]:
    return [
        node
        for node in state.nodes.values()
        if node.derived
        and node.assist_requested_by == requested_by
        and node.agent_name == agent_name
    ]


def pending_interventions(state: OrchestrationState) -> list[Intervention]:
    return [iv for iv in state.interventions.values() if iv.status == InterventionStatus.PENDING]


def pending_intervention_for(state: OrchestrationState, node_id: str) -> Intervention | None:
    for intervention in state.interventions.values():
        if intervention.node_id == node_id and intervention.status == InterventionStatus.PENDING:
            return intervention
    return None


# ----------------------------------------------------------------- transitions


def add_member(state: OrchestrationState, name: str, url: str, reason: str) -> bool:
    if name in state.members:
        return False
    state.members[name] = Member(name=name, url=url, reason=reason)
    return True


def add_intervention(
    state: OrchestrationState,
    node_id: str,
    question: str,
    *,
    question_type: QuestionType = QuestionType.INPUT,
    options: list[str] | None = None,
    multi: bool = False,
    requester: str = "",
) -> Intervention:
    intervention = Intervention(
        id=uuid.uuid4().hex,
        node_id=node_id,
        question=question,
        question_type=question_type,
        options=list(options or []),
        multi=multi,
        requester=requester,
    )
    state.interventions[intervention.id] = intervention
    return intervention


def add_cancel_request(
    state: OrchestrationState, target_node_id: str, question: str
) -> Intervention | None:
    for intervention in state.interventions.values():
        if (
            intervention.kind == "confirm_cancel"
            and intervention.status == InterventionStatus.PENDING
            and intervention.target_node_id == target_node_id
        ):
            return None
    intervention = Intervention(
        id=uuid.uuid4().hex,
        node_id=target_node_id,
        question=question,
        kind=InterventionKind.CONFIRM_CANCEL,
        question_type=QuestionType.CONFIRM,
        requester="orchestrator",
        target_node_id=target_node_id,
    )
    state.interventions[intervention.id] = intervention
    return intervention


def expire_cancel_requests(state: OrchestrationState, node_id: str) -> list[Intervention]:
    expired = []
    for intervention in state.interventions.values():
        if (
            intervention.kind == "confirm_cancel"
            and intervention.status == InterventionStatus.PENDING
            and intervention.target_node_id == node_id
        ):
            intervention.status = InterventionStatus.EXPIRED
            expired.append(intervention)
    return expired


def normalize_interventions(state: OrchestrationState) -> list[Intervention]:
    """Expire pending interventions whose target is no longer waiting."""
    expired = []
    for intervention in state.interventions.values():
        if intervention.status != InterventionStatus.PENDING:
            continue
        node = state.nodes.get(intervention.target_node_id or intervention.node_id)
        if node is None:
            intervention.status = InterventionStatus.EXPIRED
            expired.append(intervention)
            continue
        if intervention.kind == "confirm_cancel":
            if node.status not in ACTIVE_NODE_STATUSES:
                intervention.status = InterventionStatus.EXPIRED
                expired.append(intervention)
        elif node.status != NodeStatus.INPUT_REQUIRED:
            intervention.status = InterventionStatus.EXPIRED
            expired.append(intervention)
    return expired


def enqueue(
    state: OrchestrationState,
    node_id: str,
    text: str,
    sender: str = "user",
    quote_id: str | None = None,
) -> QueuedMessage:
    message = QueuedMessage(
        id=f"qm{state.next_message}", text=text, sender=sender, quote_id=quote_id
    )
    state.next_message += 1
    state.queue.setdefault(node_id, []).append(message)
    return message


def take_queued(state: OrchestrationState, node_id: str) -> list[QueuedMessage]:
    return state.queue.pop(node_id, [])


def start_new_plan(state: OrchestrationState, plan_id: str) -> None:
    """Reset plan-scoped state for a new turn, keeping session members."""
    state.plan_id = plan_id
    state.plan_version += 1
    state.nodes = {}
    state.interventions = {}
    state.queue = {}
    state.derived_count = 0
    state.patch_count = 0
    state.revision_count = 0
    state.next_message = 1


# --------------------------------------------------------------- serialization


def state_to_json(state: OrchestrationState) -> str:
    """Full snapshot: nodes, members, interventions and queue."""
    return json.dumps(
        {
            "plan_id": state.plan_id,
            "plan_version": state.plan_version,
            "nodes": [n.to_dict() for n in state.nodes.values()],
            "members": [m.to_dict() for m in state.members.values()],
            "interventions": [iv.to_dict() for iv in state.interventions.values()],
            "queue": {
                node_id: [qm.to_dict() for qm in messages]
                for node_id, messages in state.queue.items()
            },
            "derived_count": state.derived_count,
            "patch_count": state.patch_count,
            "revision_count": state.revision_count,
            "next_message": state.next_message,
        },
        ensure_ascii=False,
    )


def state_from_json(raw: str) -> OrchestrationState:
    """Rebuild state from a full snapshot."""
    parsed: object = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("state snapshot must be a JSON object")
    data = cast("dict[str, Any]", parsed)
    state = OrchestrationState()
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
        state.queue[str(node_id)] = [QueuedMessage.from_dict(message) for message in messages]
    state.derived_count = int(data.get("derived_count", 0))
    state.patch_count = int(data.get("patch_count", 0))
    state.revision_count = int(data.get("revision_count", 0))
    state.next_message = int(data.get("next_message", 1))
    return state


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
        return state_from_json(raw.string_value)
    except (ValueError, TypeError):
        return None
