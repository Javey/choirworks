from __future__ import annotations

# Mirrors ADK's _apply_rewinds (google-adk, Apache-2.0,
# https://github.com/google/adk-python,
# src/google/adk/events/_rewind_events.py).  ADK stores rewind
# markers as in-band events; ChoirWorks stores them as in-band
# A2A tasks with metadata["choirworks.rewind"] = before_task_id.
from a2a.types.a2a_pb2 import Role, Task

from choirworks.a2a.room import room_options
from choirworks.orchestration.state import OrchestrationState, load_state

REWIND_KEY = "choirworks.rewind"


class RewindUnavailable(RuntimeError):
    pass


def extract_rewind(task: Task) -> str | None:
    """Return the ``before_task_id`` if *task* is a rewind marker, else ``None``."""
    if not task.metadata.fields:
        return None
    raw = task.metadata.fields.get(REWIND_KEY)
    if raw is None or not raw.HasField("string_value") or not raw.string_value:
        return None
    return raw.string_value


def apply_rewinds(tasks_oldest_first: list[Task]) -> list[Task]:
    """Return the visible subset of *tasks_oldest_first* after rewinds.

    Iterates backward.  When a task carries ``choirworks.rewind == X``, drops
    that task together with every task between it and the task whose id is
    ``X`` (inclusive), then resumes the backward walk from there.

    Args:
        tasks_oldest_first: Full task history, oldest first.

    Returns:
        The oldest-first subset that survives all rewinds.
    """
    kept: list[Task] = []
    i = len(tasks_oldest_first) - 1
    while i >= 0:
        task = tasks_oldest_first[i]
        before_id = extract_rewind(task)
        if before_id is not None:
            for j in range(i):
                if tasks_oldest_first[j].id == before_id:
                    i = j
                    break
        else:
            kept.append(task)
        i -= 1
    kept.reverse()
    return kept


def restore_state(
    visible_tasks_oldest_first: list[Task],
    before_task_id: str,
) -> OrchestrationState:
    """Restore the state snapshot from the predecessor of *before_task_id*.

    *visible_tasks_oldest_first* must already have rewinds applied (see
    :func:`apply_rewinds`).  Returns an empty :class:`OrchestrationState`
    when *before_task_id* is the first visible turn.
    """
    index = {
        task.id: position for position, task in enumerate(visible_tasks_oldest_first)
    }
    pos = index.get(before_task_id)
    if pos is None or pos == 0:
        return OrchestrationState()
    checkpoint = visible_tasks_oldest_first[pos - 1]
    state = load_state(checkpoint)
    if state is None:
        raise RewindUnavailable(f"no state snapshot for task {checkpoint.id}")
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
