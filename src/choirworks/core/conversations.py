from __future__ import annotations

from typing import Any

from choirworks.models.enums import TERMINAL_TASK_STATUSES, NodeStatus
from choirworks.store import projections

RESULT_TRUNCATE = 1200


def _artifacts_text(output: dict[str, Any] | None) -> str:
    if not output:
        return ""
    parts = [
        artifact.get("text", "")
        for artifact in output.get("artifacts", [])
        if isinstance(artifact, dict)
    ]
    return " ".join(part for part in parts if part).strip()


async def build_conversation_context(
    db: Any,
    conversation_id: str,
    exclude_task_id: str | None,
    *,
    max_tasks: int = 5,
    max_chars: int = 4000,
) -> str | None:
    task_ids = await projections.fetch_task_ids_for_conversation(db, conversation_id)
    entries: list[tuple[str, str]] = []
    for task_id in task_ids:
        if task_id == exclude_task_id:
            continue
        task = await projections.fetch_task(db, task_id)
        if task is None or task.status not in TERMINAL_TASK_STATUSES:
            continue
        plan = await projections.fetch_current_plan(db, task_id)
        if plan is None:
            continue
        nodes = await projections.fetch_nodes(db, task_id, plan.id)
        texts = [
            _artifacts_text(node.output)
            for node in nodes
            if node.status is NodeStatus.COMPLETED
        ]
        result = " ".join(text for text in texts if text).strip()
        if len(result) > RESULT_TRUNCATE:
            result = result[:RESULT_TRUNCATE]
        entries.append((task.request, result))

    entries = entries[-max_tasks:]
    while entries:
        context = "\n\n".join(
            f"User: {request}\nResult: {result}" for request, result in entries
        )
        if len(context) <= max_chars:
            return context
        entries = entries[1:]
    return None
