from __future__ import annotations

from a2a.types import TaskState

from agent_hub.models.enums import EventType, NodeStatus


def _artifact_text(artifact) -> str:
    return "\n".join(part.text for part in artifact.parts if part.HasField("text"))


async def reconcile_once(db, events, remote) -> int:
    """用远程 GetTask 校正本地漂移；返回修正的节点数。"""
    fixed = 0
    cursor = await db.conn.execute(
        "SELECT id, task_id, agent_url, a2a_task_id, status FROM nodes"
    )
    for row in await cursor.fetchall():
        if row["status"] not in (NodeStatus.DISPATCHED.value, NodeStatus.WORKING.value):
            continue
        if not row["a2a_task_id"] or not row["agent_url"]:
            continue
        task = await remote.get_task(row["agent_url"], row["a2a_task_id"])
        if task is None:
            continue
        state = task.status.state
        if state is TaskState.TASK_STATE_COMPLETED:
            artifacts = [
                {
                    "id": artifact.artifact_id,
                    "name": artifact.name,
                    "text": _artifact_text(artifact),
                }
                for artifact in task.artifacts
            ]
            await events.append(
                row["task_id"],
                EventType.NODE_STATE_CHANGED,
                {
                    "node_id": row["id"],
                    "from": row["status"],
                    "to": NodeStatus.COMPLETED.value,
                },
            )
            await events.append(
                row["task_id"],
                EventType.NODE_OUTPUT,
                {"node_id": row["id"], "output": {"artifacts": artifacts}},
            )
            fixed += 1
        elif state in (
            TaskState.TASK_STATE_FAILED,
            TaskState.TASK_STATE_REJECTED,
            TaskState.TASK_STATE_CANCELED,
        ):
            target = (
                NodeStatus.CANCELED
                if state is TaskState.TASK_STATE_CANCELED
                else NodeStatus.FAILED
            )
            await events.append(
                row["task_id"],
                EventType.NODE_STATE_CHANGED,
                {"node_id": row["id"], "from": row["status"], "to": target.value},
            )
            fixed += 1
        elif state in (
            TaskState.TASK_STATE_INPUT_REQUIRED,
            TaskState.TASK_STATE_AUTH_REQUIRED,
        ):
            await events.append(
                row["task_id"],
                EventType.NODE_STATE_CHANGED,
                {
                    "node_id": row["id"],
                    "from": row["status"],
                    "to": NodeStatus.INPUT_REQUIRED.value,
                },
            )
            fixed += 1
    return fixed
