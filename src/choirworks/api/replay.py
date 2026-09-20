from __future__ import annotations

from typing import Any

from a2a.types.a2a_pb2 import Task
from google.protobuf.json_format import MessageToDict


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
        task_dicts.append(task_dict)

    events: list[dict[str, Any]] = []

    for task_dict in task_dicts:
        task_only = {k: v for k, v in task_dict.items() if k != "artifacts"}
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
                    merged_art = {
                        "artifactId": art.get("artifactId", ""),
                        "name": art.get("name", ""),
                        "parts": [{"text": merged_text}],
                    }
                    events.append({
                        "artifactUpdate": {
                            "taskId": task_id,
                            "contextId": context_id,
                            "artifact": merged_art,
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
