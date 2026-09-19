from types import SimpleNamespace

from a2a.helpers import new_data_message, new_text_message
from a2a.types.a2a_pb2 import Role, Task, TaskState, TaskStatus
from google.protobuf.json_format import ParseDict

from choirworks.a2a.executor import _is_resume_message
from choirworks.a2a.recovery import recover_tasks
from choirworks.a2a.state import OrchestrationState


def _resume_message():
    message = new_data_message(
        {"kind": "resume"}, role=Role.ROLE_USER, task_id="t1", context_id="c1"
    )
    ParseDict({"choirworks.resume": {"kind": "resume"}}, message.metadata)
    return message


def _persisted_task(task_id: str = "t1", context_id: str = "c1") -> Task:
    state = OrchestrationState(plan_id="p1")
    task = Task(
        id=task_id,
        context_id=context_id,
        status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
    )
    ParseDict({"choirworks.state": state.to_json()}, task.metadata)
    return task


class FakeContextStore:
    def __init__(
        self, records: dict[str, str], markers: dict[str, str] | None = None
    ):
        self.records = records
        self.markers = markers or {}

    async def get(self, context_id):  # noqa: ANN001, ANN201
        raw = self.records.get(context_id)
        if raw is None:
            return None
        return SimpleNamespace(
            state=raw, rewind_markers=self.markers.get(context_id, "[]")
        )

    async def list(self):  # noqa: ANN201
        return [
            SimpleNamespace(
                context_id=context_id,
                state=raw,
                rewind_markers=self.markers.get(context_id, "[]"),
            )
            for context_id, raw in self.records.items()
        ]


class FakeTaskStore:
    def __init__(self, tasks: list[Task]):
        self.tasks = tasks

    async def list(self, request, context):  # noqa: ANN001, ANN201
        return SimpleNamespace(tasks=self.tasks, next_page_token="")


class RecordingHandler:
    def __init__(self):
        self.requests = []

    async def on_message_send(self, request, context):  # noqa: ANN001, ANN201
        self.requests.append(request)
        return SimpleNamespace()


def test_is_resume_message_detects_data_message():
    assert _is_resume_message(_resume_message()) is True
    assert (
        _is_resume_message(new_text_message("你好", role=Role.ROLE_USER)) is False
    )


async def test_recover_tasks_sends_structured_resume_message():
    handler = RecordingHandler()
    recovered = await recover_tasks(handler, FakeTaskStore([_persisted_task()]))

    assert recovered == 1
    [request] = handler.requests
    message = request.message
    assert message.task_id == "t1"
    assert "choirworks.resume" in message.metadata.fields
    assert message.parts
    assert all(not part.HasField("text") for part in message.parts)


async def test_recover_tasks_uses_context_state_without_task_snapshot():
    handler = RecordingHandler()
    task = Task(
        id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
    )
    contexts = FakeContextStore({"c1": OrchestrationState(plan_id="p1").to_json()})

    recovered = await recover_tasks(handler, FakeTaskStore([task]), contexts)

    assert recovered == 1
    assert handler.requests[0].message.task_id == "t1"


async def test_recover_tasks_skips_without_state_or_snapshot():
    handler = RecordingHandler()
    task = Task(
        id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
    )

    recovered = await recover_tasks(
        handler, FakeTaskStore([task]), FakeContextStore({})
    )

    assert recovered == 0
    assert handler.requests == []


async def test_recover_tasks_skips_hidden_tasks():
    import json

    handler = RecordingHandler()
    hidden_id = "t2"
    contexts = FakeContextStore(
        {"c1": OrchestrationState(plan_id="p1").to_json()},
        markers={
            "c1": json.dumps([
                {"before_task_id": hidden_id, "cut_task_id": hidden_id}
            ])
        },
    )

    recovered = await recover_tasks(
        handler,
        FakeTaskStore([_persisted_task(hidden_id), _persisted_task("t1")]),
        contexts,
    )

    assert recovered == 1
    assert handler.requests[0].message.task_id == "t1"


async def test_recover_tasks_resumes_once_per_context():
    handler = RecordingHandler()
    recovered = await recover_tasks(
        handler,
        FakeTaskStore(
            [
                _persisted_task("t2", "c1"),
                _persisted_task("t1", "c1"),
                _persisted_task("t3", "c2"),
            ]
        ),
    )

    assert recovered == 2
    assert [r.message.context_id for r in handler.requests] == ["c1", "c2"]
    assert [r.message.task_id for r in handler.requests] == ["t2", "t3"]
