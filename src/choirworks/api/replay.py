from __future__ import annotations

from typing import TypedDict

from a2a.types.a2a_pb2 import Task
from google.protobuf.json_format import MessageToDict


class MetadataJson(TypedDict, total=False):
    cw_thought: bool
    cw_type: str
    author: str


class PartJson(TypedDict, total=False):
    text: str
    raw: str
    url: str
    data: object
    metadata: MetadataJson
    filename: str
    mediaType: str


class ArtifactJson(TypedDict, total=False):
    artifactId: str
    name: str
    description: str
    parts: list[PartJson]
    metadata: MetadataJson
    extensions: list[str]


class MessageJson(TypedDict, total=False):
    messageId: str
    contextId: str
    taskId: str
    role: int
    parts: list[PartJson]
    metadata: dict[str, object]
    extensions: list[str]
    referenceTaskIds: list[str]


class StatusJson(TypedDict, total=False):
    state: int
    message: MessageJson
    timestamp: str


class TaskJson(TypedDict, total=False):
    id: str
    contextId: str
    status: StatusJson
    artifacts: list[ArtifactJson]
    history: list[MessageJson]
    metadata: dict[str, object]


class NodeStateJson(TypedDict, total=False):
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


class MemberJson(TypedDict, total=False):
    name: str
    agent_name: str
    url: str
    agent_url: str
    reason: str
    joined_at: str


class InterventionJson(TypedDict, total=False):
    id: str
    intervention_id: str
    node_id: str
    question: str
    status: str
    answer: str | None
    responder: str | None
    kind: str
    target_node_id: str | None
    created_at: str


class ContextJson(TypedDict, total=False):
    plan_id: str
    plan_version: int
    nodes: list[NodeStateJson]
    members: list[MemberJson]
    interventions: list[InterventionJson]
    queue: dict[str, list[object]]
    derived_count: int
    patch_count: int
    next_intervention: int
    next_message: int


class StatusUpdateJson(TypedDict, total=False):
    taskId: str
    contextId: str
    status: StatusJson
    metadata: dict[str, object]


class ArtifactUpdateJson(TypedDict, total=False):
    taskId: str
    contextId: str
    artifact: ArtifactJson
    append: bool
    lastChunk: bool
    metadata: dict[str, object]


def _state_delta_event(
    context: ContextJson,
    context_id: str,
    task_id: str,
    task_state: int,
) -> StatusUpdateJson:
    nodes: dict[str, object] = {}
    for n in context.get("nodes", []):
        node_id = n.get("id", "")
        if not node_id:
            continue
        nodes[node_id] = {
            "status": n.get("status", "pending"),
            "output": n.get("output") or "",
            "error": n.get("error") or "",
            "name": n.get("name", ""),
            "agent_name": n.get("agent_name", ""),
        }

    members: list[object] = list(context.get("members", []))

    interventions: dict[str, object] = {}
    for iv in context.get("interventions", []):
        iv_id = iv.get("id") or iv.get("intervention_id") or ""
        if not iv_id:
            continue
        interventions[iv_id] = {
            "status": iv.get("status", "pending"),
            "node_id": iv.get("node_id", ""),
            "kind": iv.get("kind", "question"),
            "question": iv.get("question", ""),
        }

    return {
        "taskId": task_id,
        "contextId": context_id,
        "status": {"state": task_state},
        "metadata": {
            "kind": "state_delta",
            "nodes": nodes,
            "members": members,
            "interventions": interventions,
        },
    }


def synthesize_replay_events(
    tasks: list[Task],
    context_id: str,
    context: ContextJson | None,
    hidden_ids: set[str] | None = None,
) -> list[dict[str, object]]:
    hidden = hidden_ids or set()
    visible = [t for t in tasks if t.id not in hidden]
    if not visible:
        return []

    task_dicts: list[TaskJson] = [
        MessageToDict(task, use_integers_for_enums=True) for task in visible
    ]

    events: list[dict[str, object]] = []

    for task_dict in task_dicts:
        task_only: dict[str, object] = {
            k: v for k, v in task_dict.items() if k != "artifacts"
        }
        events.append({"task": task_only})

    for task_dict in task_dicts:
        task_id = task_dict.get("id", "")
        for art in task_dict.get("artifacts", []):
            parts = art.get("parts", [])
            if not parts:
                continue
            p_meta = parts[0].get("metadata", {})
            is_thought = p_meta.get("cw_thought") is True
            is_fc = p_meta.get("cw_type") == "function_call"
            if is_thought or is_fc:
                events.append({
                    "artifactUpdate": {
                        "taskId": task_id,
                        "contextId": context_id,
                        "artifact": art,
                        "append": False,
                        "lastChunk": True,
                        "metadata": {},
                    }
                })
            else:
                merged_text = "".join(
                    p.get("text", "")
                    for p in parts
                    if "text" in p
                )
                if merged_text:
                    events.append({
                        "artifactUpdate": {
                            "taskId": task_id,
                            "contextId": context_id,
                            "artifact": {
                                "artifactId": art.get("artifactId", ""),
                                "name": art.get("name", ""),
                                "parts": [{"text": merged_text}],
                            },
                            "append": False,
                            "lastChunk": True,
                            "metadata": {},
                        }
                    })

    if context and task_dicts:
        last_dict = task_dicts[-1]
        task_state = last_dict.get("status", {}).get("state", 0)
        events.append({
            "statusUpdate": _state_delta_event(
                context, context_id, last_dict.get("id", ""), task_state
            )
        })

    return events
