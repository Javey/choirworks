from __future__ import annotations

from a2a.types.a2a_pb2 import Role, Task

from choirworks.a2a.room import room_options
from choirworks.orchestration.state import OrchestrationState, load_state


class RewindUnavailable(RuntimeError):
    pass


def restore_state(
    visible_tasks_oldest_first: list[Task],
    before_task_id: str,
) -> OrchestrationState:
    """Restore the state snapshot from the predecessor of *before_task_id*.

    *visible_tasks_oldest_first* must already have rewinds applied (see
    :func:`choirworks.a2a.tasks.iter_all_tasks`).  Returns an empty
    :class:`OrchestrationState` when *before_task_id* is the first visible
    turn.
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
