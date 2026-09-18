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
    ParseDict({"choirworks.state": state.to_minimal_json()}, task.metadata)
    return task


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
