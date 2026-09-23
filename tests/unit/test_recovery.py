from types import SimpleNamespace

from a2a.helpers import new_text_message
from a2a.server.agent_execution import RequestContext
from a2a.server.context import ServerCallContext
from a2a.types.a2a_pb2 import (
    Role,
    SendMessageRequest,
    Task,
    TaskState,
    TaskStatus,
)
from google.protobuf.json_format import ParseDict

from choirworks.a2a.executor import _is_recover_request
from choirworks.a2a.recovery import recover_tasks
from choirworks.a2a.tasks import RECOVER_KEY, REWIND_KEY
from choirworks.orchestration.state import OrchestrationState, state_to_json


def _request_context(message=None, *, state=None):  # noqa: ANN001, ANN202
    return RequestContext(
        call_context=ServerCallContext(state=state or {}),
        request=SendMessageRequest(message=message) if message else None,
    )


def _persisted_task(task_id: str = "t1", context_id: str = "c1") -> Task:
    state = OrchestrationState(plan_id="p1")
    task = Task(
        id=task_id,
        context_id=context_id,
        status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
    )
    ParseDict({"choirworks.state": state_to_json(state)}, task.metadata)
    return task


def _rewind_marker(before_task_id: str, context_id: str = "c1") -> Task:
    task = Task(
        id=f"rewind-{before_task_id}",
        context_id=context_id,
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
    )
    ParseDict({REWIND_KEY: before_task_id}, task.metadata)
    return task


class FakeContextStore:
    def __init__(self, records: dict[str, str]):
        self.records = records

    async def get(self, context_id):  # noqa: ANN001, ANN201
        raw = self.records.get(context_id)
        if raw is None:
            return None
        return SimpleNamespace(state=raw)

    async def list(self):  # noqa: ANN201
        return [
            SimpleNamespace(context_id=context_id, state=raw)
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
        self.contexts = []

    async def on_message_send(self, request, context):  # noqa: ANN001, ANN201
        self.requests.append(request)
        self.contexts.append(context)
        return SimpleNamespace()


def test_is_recover_request_requires_internal_call_context():
    assert _is_recover_request(_request_context()) is False
    assert _is_recover_request(_request_context(state={RECOVER_KEY: True})) is True


def test_is_recover_request_ignores_client_metadata():
    message = new_text_message("你好", role=Role.ROLE_USER)
    ParseDict({RECOVER_KEY: {"kind": "recover"}}, message.metadata)
    assert _is_recover_request(_request_context(message)) is False


async def test_recover_tasks_sends_internal_recover_request():
    handler = RecordingHandler()
    recovered = await recover_tasks(
        handler, FakeTaskStore([_persisted_task()]), FakeContextStore({})
    )

    assert recovered == 1
    [request] = handler.requests
    message = request.message
    assert message.task_id == "t1"
    assert RECOVER_KEY not in message.metadata.fields
    assert message.parts
    assert all(not part.HasField("text") for part in message.parts)
    [context] = handler.contexts
    assert context.state[RECOVER_KEY] is True


async def test_recover_tasks_uses_context_state_without_task_snapshot():
    handler = RecordingHandler()
    task = Task(
        id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
    )
    contexts = FakeContextStore({"c1": state_to_json(OrchestrationState(plan_id="p1"))})

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

    recovered = await recover_tasks(handler, FakeTaskStore([task]), FakeContextStore({}))

    assert recovered == 0
    assert handler.requests == []


async def test_recover_tasks_skips_hidden_tasks():
    handler = RecordingHandler()
    # t2 is hidden by a rewind marker, t1 is visible.
    tasks = [
        _rewind_marker("t2"),
        _persisted_task("t2"),
        _persisted_task("t1"),
    ]
    # FakeTaskStore returns newest-first; recovery uses list_all_tasks which
    # iterates in store order, so order matters.
    contexts = FakeContextStore({"c1": state_to_json(OrchestrationState(plan_id="p1"))})

    recovered = await recover_tasks(
        handler,
        FakeTaskStore(tasks),
        contexts,
    )

    assert recovered == 1
    assert handler.requests[0].message.task_id == "t1"


async def test_recover_tasks_recovers_once_per_context():
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
        FakeContextStore({}),
    )

    assert recovered == 2
    assert [r.message.context_id for r in handler.requests] == ["c1", "c2"]
    assert [r.message.task_id for r in handler.requests] == ["t2", "t3"]
