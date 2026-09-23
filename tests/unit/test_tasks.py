from __future__ import annotations

from a2a.server.context import ServerCallContext
from a2a.server.tasks.task_store import TaskStore
from a2a.types.a2a_pb2 import (
    ListTasksRequest,
    ListTasksResponse,
    Task,
    TaskState,
    TaskStatus,
)
from google.protobuf.json_format import ParseDict

from choirworks.a2a.tasks import REWIND_KEY, list_all_tasks


def _task(
    task_id: str,
    context_id: str = "c1",
    state: TaskState = TaskState.TASK_STATE_COMPLETED,
) -> Task:
    return Task(
        id=task_id,
        context_id=context_id,
        status=TaskStatus(state=state),
    )


def _rewind_task(before_task_id: str, context_id: str = "c1") -> Task:
    task = _task(f"rewind-{before_task_id}", context_id)
    ParseDict({REWIND_KEY: before_task_id}, task.metadata)
    return task


class FakeTaskStore(TaskStore):
    def __init__(self, tasks: list[Task]):
        # Store oldest-first for predictable test construction;
        # DatabaseTaskStore returns newest-first.
        self._tasks = list(reversed(tasks))

    async def save(self, task: Task, context: ServerCallContext) -> None:
        pass

    async def get(self, task_id: str, context: ServerCallContext) -> Task | None:
        return next((t for t in self._tasks if t.id == task_id), None)

    async def list(
        self,
        params: ListTasksRequest,
        context: ServerCallContext,
    ) -> ListTasksResponse:
        tasks = self._tasks
        if params.context_id:
            tasks = [t for t in tasks if t.context_id == params.context_id]
        return ListTasksResponse(tasks=tasks, next_page_token="")

    async def delete(self, task_id: str, context: ServerCallContext) -> None:
        pass


async def test_list_all_tasks_no_markers():
    tasks = [_task("a"), _task("b"), _task("c")]
    result = await list_all_tasks(FakeTaskStore(tasks))
    assert [t.id for t in result] == ["c", "b", "a"]


async def test_list_all_tasks_reverse():
    tasks = [_task("a"), _task("b"), _task("c")]
    result = await list_all_tasks(FakeTaskStore(tasks), reverse=True)
    assert [t.id for t in result] == ["a", "b", "c"]


async def test_list_all_tasks_filters_rewind_range():
    # [a, b, c, REWIND(b), d] — b, c, and marker hidden; a and d visible.
    tasks = [_task("a"), _task("b"), _task("c"), _rewind_task("b"), _task("d")]
    result = await list_all_tasks(FakeTaskStore(tasks), reverse=True)
    assert [t.id for t in result] == ["a", "d"]


async def test_list_all_tasks_drops_marker_when_before_is_oldest():
    tasks = [_task("a"), _task("b"), _rewind_task("a")]
    result = await list_all_tasks(FakeTaskStore(tasks), reverse=True)
    assert [t.id for t in result] == []


async def test_list_all_tasks_chained_rewinds():
    # [a, b, c, REWIND(c), d, REWIND(d), e] → visible: a, b, e
    tasks = [
        _task("a"),
        _task("b"),
        _task("c"),
        _rewind_task("c"),
        _task("d"),
        _rewind_task("d"),
        _task("e"),
    ]
    result = await list_all_tasks(FakeTaskStore(tasks), reverse=True)
    assert [t.id for t in result] == ["a", "b", "e"]


async def test_list_all_tasks_independent_contexts():
    # Rewind in context A doesn't affect context B.
    tasks = [
        _task("a1", context_id="A"),
        _task("a2", context_id="A"),
        _rewind_task("a1", context_id="A"),
        _task("b1", context_id="B"),
        _task("b2", context_id="B"),
    ]
    result = await list_all_tasks(FakeTaskStore(tasks), reverse=True)
    result_by_ctx: dict[str, list[str]] = {}
    for t in result:
        result_by_ctx.setdefault(t.context_id, []).append(t.id)
    # REWIND(a1) hides a1 and a2; B is untouched.
    assert "A" not in result_by_ctx
    assert result_by_ctx["B"] == ["b1", "b2"]


async def test_list_all_tasks_with_context_id_filter():
    tasks = [
        _task("a1", context_id="A"),
        _task("a2", context_id="A"),
        _task("a3", context_id="A"),
        _rewind_task("a2", context_id="A"),
        _task("b1", context_id="B"),
    ]
    result = await list_all_tasks(FakeTaskStore(tasks), context_id="A", reverse=True)
    # REWIND(a2) hides a2 and a3; a1 stays visible.
    assert [t.id for t in result] == ["a1"]
