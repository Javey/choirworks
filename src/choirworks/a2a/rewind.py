from __future__ import annotations

import json
from dataclasses import dataclass

from a2a.types.a2a_pb2 import Role, Task

from choirworks.a2a.room import room_options
from choirworks.a2a.state import OrchestrationState, load_state


class RewindUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class RewindMarker:
    before_task_id: str
    cut_task_id: str
    created_at: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "before_task_id": self.before_task_id,
            "cut_task_id": self.cut_task_id,
            "created_at": self.created_at,
        }


def parse_markers(raw: str) -> list[RewindMarker]:
    try:
        items = json.loads(raw)
    except ValueError:
        return []
    if not isinstance(items, list):
        return []
    markers: list[RewindMarker] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        before = item.get("before_task_id")
        cut = item.get("cut_task_id")
        if not before or not cut:
            continue
        markers.append(
            RewindMarker(
                before_task_id=str(before),
                cut_task_id=str(cut),
                created_at=str(item.get("created_at", "")),
            )
        )
    return markers


def hidden_task_ids(task_ids: list[str], markers: list[RewindMarker]) -> set[str]:
    """Tasks hidden by rewinds; ``task_ids`` ordered oldest first."""
    index = {task_id: position for position, task_id in enumerate(task_ids)}
    hidden: set[str] = set()
    for marker in markers:
        start = index.get(marker.before_task_id)
        end = index.get(marker.cut_task_id)
        if start is None or end is None:
            continue
        if start > end:
            start, end = end, start
        hidden.update(task_ids[start : end + 1])
    return hidden


def checkpoint_before(
    task_ids: list[str],
    markers: list[RewindMarker],
    before_task_id: str,
) -> str | None:
    """Task whose snapshot holds the state before ``before_task_id``.

    Skips tasks hidden by earlier rewinds; when the walk lands inside a hidden
    range it jumps back to before that rewind's target. ``None`` means the
    target is the first live turn, so the state is empty.
    """
    index = {task_id: position for position, task_id in enumerate(task_ids)}
    hidden = hidden_task_ids(task_ids, markers)
    position = index[before_task_id] - 1
    while position >= 0:
        candidate = task_ids[position]
        if candidate not in hidden:
            return candidate
        covering = [
            marker
            for marker in markers
            if marker.before_task_id in index
            and marker.cut_task_id in index
            and index[marker.before_task_id] <= position <= index[marker.cut_task_id]
        ]
        position = min(index[marker.before_task_id] for marker in covering) - 1
    return None


def restore_state(
    tasks_oldest_first: list[Task],
    markers: list[RewindMarker],
    before_task_id: str,
) -> OrchestrationState:
    checkpoint_id = checkpoint_before(
        [task.id for task in tasks_oldest_first], markers, before_task_id
    )
    if checkpoint_id is None:
        return OrchestrationState()
    task = next(task for task in tasks_oldest_first if task.id == checkpoint_id)
    state = load_state(task)
    if state is None:
        raise RewindUnavailable(f"no state snapshot for task {checkpoint_id}")
    return state


def is_human_turn(task: Task) -> bool:
    """Whether the task was started by a human room message."""
    if not task.history:
        return False
    message = task.history[0]
    if message.role != Role.ROLE_USER:
        return False
    if message.metadata.fields and "choirworks.resume" in message.metadata.fields:
        return False
    sender = room_options(message).get("sender")
    return sender in (None, "", "user")
