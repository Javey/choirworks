from __future__ import annotations

from typing import Any

from a2a.types.a2a_pb2 import Task
from google.protobuf.json_format import MessageToDict


def _part_to_wire(part: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if part.get("text"):
        result["content"] = {"$case": "text", "value": part["text"]}
    elif part.get("raw"):
        result["content"] = {"$case": "raw", "value": part["raw"]}
    elif part.get("url"):
        result["content"] = {"$case": "url", "value": part["url"]}
    elif "data" in part:
        result["content"] = {"$case": "data", "value": part["data"]}
    else:
        result["content"] = {"$case": "text", "value": ""}
    if "metadata" in part:
        result["metadata"] = part["metadata"]
    if "filename" in part:
        result["filename"] = part["filename"]
    if "mediaType" in part:
        result["mediaType"] = part["mediaType"]
    return result


def _convert_parts_in_place(container: dict[str, Any]) -> None:
    if "parts" in container:
        container["parts"] = [_part_to_wire(p) for p in container["parts"]]
    for msg in container.get("history", []):
        if "parts" in msg:
            msg["parts"] = [_part_to_wire(p) for p in msg["parts"]]
    for art in container.get("artifacts", []):
        if "parts" in art:
            art["parts"] = [_part_to_wire(p) for p in art["parts"]]


def _state_delta_event(
    context: dict[str, Any],
    context_id: str,
    task_id: str,
    task_state: int,
) -> dict[str, Any]:
    nodes: dict[str, Any] = {}
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

    members = context.get("members", [])

    interventions: dict[str, Any] = {}
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
        "payload": {
            "$case": "statusUpdate",
            "value": {
                "taskId": task_id,
                "contextId": context_id,
                "status": {"state": task_state},
                "metadata": {
                    "kind": "state_delta",
                    "nodes": nodes,
                    "members": members,
                    "interventions": interventions,
                },
            },
        }
    }


def synthesize_replay_events(
    tasks: list[Task],
    context_id: str,
    context: dict[str, Any] | None,
    hidden_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    hidden = hidden_ids or set()
    visible = [t for t in tasks if t.id not in hidden]
    if not visible:
        return []

    task_dicts: list[dict[str, Any]] = []
    for task in visible:
        task_dict = MessageToDict(task, use_integers_for_enums=True)
        _convert_parts_in_place(task_dict)
        task_dicts.append(task_dict)

    events: list[dict[str, Any]] = []

    for task_dict in task_dicts:
        events.append({"payload": {"$case": "task", "value": task_dict}})

    for task_dict in task_dicts:
        task_id = task_dict.get("id", "")
        for art in task_dict.get("artifacts", []):
            parts = art.get("parts", [])
            if not parts:
                continue
            p_meta = parts[0].get("metadata", {})
            if p_meta.get("cw_thought") is True or p_meta.get("cw_type") == "function_call":
                events.append({
                    "payload": {
                        "$case": "artifactUpdate",
                        "value": {
                            "taskId": task_id,
                            "contextId": context_id,
                            "artifact": art,
                            "append": False,
                            "lastChunk": True,
                            "metadata": {},
                        },
                    }
                })

    if context and task_dicts:
        last_dict = task_dicts[-1]
        task_state = last_dict.get("status", {}).get("state", 0)
        events.append(
            _state_delta_event(
                context, context_id, last_dict.get("id", ""), task_state
            )
        )

    return events
