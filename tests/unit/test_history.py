from __future__ import annotations

from types import SimpleNamespace

from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    Role,
    Task,
    TaskArtifactUpdateEvent,
)
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict
from litellm.types.llms.openai import (
    ChatCompletionToolMessage,
    OpenAIChatCompletionAssistantMessage,
    OpenAIChatCompletionUserMessage,
)
from pydantic import BaseModel

from choirworks.core.events import CwType
from choirworks.core.tool import FunctionResult, FunctionTool
from choirworks.orchestration.events import emit_function_call, record_message
from choirworks.orchestration.history import HistoryBuilder
from choirworks.orchestration.state import OrchestrationState


class _FakeTaskStore:
    """Minimal in-memory task store for rebuild tests."""

    def __init__(self, tasks: list[Task]):
        self._tasks = tasks

    async def list(self, params, context):  # type: ignore[no-untyped-def]
        del context
        from a2a.types.a2a_pb2 import ListTasksResponse

        cid = params.context_id if params.context_id else ""
        filtered = [t for t in self._tasks if not cid or t.context_id == cid]
        return ListTasksResponse(tasks=filtered, next_page_token="")


def _make_task(task_id: str, context_id: str, history: list[tuple[Role, str]]) -> Task:
    msgs = []
    for role, text in history:
        msgs.append(
            Message(parts=[Part(text=text)], role=role, task_id=task_id, context_id=context_id)
        )
    return Task(id=task_id, context_id=context_id, history=msgs)


def _make_compaction_artifact(compacted_count: int, summary: str) -> Artifact:
    part = Part(text=summary)
    meta = struct_pb2.Struct()
    meta.update({"cw_type": CwType.COMPACTION, "compacted_count": compacted_count})
    part.metadata.CopyFrom(meta)
    return Artifact(artifact_id="compaction", parts=[part])


def _make_decision_artifact(text: str, role: str = "assistant") -> Artifact:
    part = Part(text=text)
    meta = struct_pb2.Struct()
    meta.update({"cw_type": CwType.DECISION, "role": role})
    part.metadata.CopyFrom(meta)
    return Artifact(artifact_id="decision", parts=[part])


def _make_history_artifact(text: str, role: str = "user") -> Artifact:
    part = Part(text=text)
    meta = struct_pb2.Struct()
    meta.update({"cw_type": CwType.HISTORY, "role": role})
    part.metadata.CopyFrom(meta)
    return Artifact(artifact_id="history", parts=[part])


def _make_function_call_artifact(
    *,
    function_name: str = "create_plan",
    call_id: str = "call-1",
    args: dict[str, object] | None = None,
    result: dict[str, object] | None = None,
    args_json: str = "{}",
    result_json: str = '{"success":true,"data":null,"error":null}',
) -> Artifact:
    data = struct_pb2.Value()
    ParseDict(
        {
            "function_name": function_name,
            "function_args": args if args is not None else {},
            "function_result": result if result is not None else {"success": True},
            "call_id": call_id,
            "args_json": args_json,
            "result_json": result_json,
        },
        data,
    )
    part = Part()
    part.data.CopyFrom(data)
    meta = struct_pb2.Struct()
    meta.update({"cw_type": CwType.FUNCTION_CALL})
    part.metadata.CopyFrom(meta)
    return Artifact(artifact_id="function-call", parts=[part])


class _StubLLM:
    def __init__(
        self,
        summary: str = "摘要内容",
        token_count: int = 0,
        context_window: int = 128000,
    ):
        self._summary = summary
        self._token_count = token_count
        self._context_window = context_window
        self.text_calls: list[dict[str, str]] = []

    async def text(self, *, system: str, user: str) -> str:
        self.text_calls.append({"system": system, "user": user})
        return self._summary

    def count_tokens(self, text: str) -> int:
        return self._token_count

    def get_context_window(self) -> int:
        return self._context_window


# ------------------------------------------------------------- rebuild


async def test_rebuild_reads_decision_artifacts():
    task = _make_task("t1", "c1", [])
    task.artifacts.append(_make_decision_artifact("[planner] plan"))
    task.artifacts.append(_make_decision_artifact("[writer] done"))
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore([task]), retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert messages == [
        {"role": "assistant", "content": "[planner] plan"},
        {"role": "assistant", "content": "[writer] done"},
    ]


async def test_rebuild_reads_history_messages():
    task = _make_task("t1", "c1", [])
    task.artifacts.append(_make_history_artifact("请确认？", role="assistant"))
    task.artifacts.append(_make_history_artifact("确认", role="user"))
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore([task]), retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert messages == [
        {"role": "assistant", "content": "请确认？"},
        {"role": "user", "content": "确认"},
    ]


async def test_rebuild_replays_function_call_pairs():
    task = _make_task("t1", "c1", [])
    task.artifacts.append(
        _make_function_call_artifact(
            call_id="call-1",
            args={"nodes": []},
            result={"success": True, "data": None, "error": None},
            args_json='{"nodes":[]}',
            result_json='{"success":true,"data":null,"error":null}',
        )
    )
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore([task]), retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert len(messages) == 2
    call = messages[0]
    assert call["role"] == "assistant"
    assert call["content"] is None
    assert call["tool_calls"][0]["id"] == "call-1"  # type: ignore[index]
    assert call["tool_calls"][0]["function"]["name"] == "create_plan"  # type: ignore[index]
    assert call["tool_calls"][0]["function"]["arguments"] == '{"nodes":[]}'  # type: ignore[index]
    tool = messages[1]
    assert tool["role"] == "tool"
    assert tool["tool_call_id"] == "call-1"
    assert tool["content"] == '{"success":true,"data":null,"error":null}'


async def test_rebuild_preserves_artifact_order():
    task = _make_task("t1", "c1", [])
    task.artifacts.append(_make_history_artifact("任务A", role="user"))
    task.artifacts.append(_make_function_call_artifact())
    task.artifacts.append(_make_decision_artifact("[outcome:n1] deliver"))
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore([task]), retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert [msg["role"] for msg in messages] == ["user", "assistant", "tool", "assistant"]


async def test_rebuild_ignores_task_history():
    tasks = [
        _make_task(
            "t1",
            "c1",
            [(Role.ROLE_USER, "进度如何"), (Role.ROLE_AGENT, "正在处理")],
        )
    ]
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore(tasks), retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert messages == []


async def test_rebuild_no_user_duplication():
    task = _make_task("t1", "c1", [(Role.ROLE_USER, "任务A")])
    task.artifacts.append(_make_history_artifact("任务A", role="user"))
    task.artifacts.append(_make_decision_artifact("[planner] plan"))
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore([task]), retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert [msg["content"] for msg in messages] == ["任务A", "[planner] plan"]


async def test_rebuild_excludes_task():
    first = _make_task("t1", "c1", [])
    first.artifacts.append(_make_decision_artifact("你好", role="user"))
    second = _make_task("t2", "c1", [])
    second.artifacts.append(_make_decision_artifact("排除我", role="user"))
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore([second, first]), retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1", exclude_task_id="t2")

    assert messages == [{"role": "user", "content": "你好"}]


async def test_rebuild_empty_context():
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore([]), retention=10)  # type: ignore[arg-type]
    messages = await builder.rebuild("")
    assert messages == []


async def test_rebuild_applies_compaction():
    task = _make_task("t1", "c1", [])
    task.artifacts.append(_make_decision_artifact("m0", role="user"))
    task.artifacts.append(_make_decision_artifact("m1"))
    task.artifacts.append(_make_decision_artifact("m2"))
    task.artifacts.append(_make_compaction_artifact(2, "早期摘要"))
    task.artifacts.append(_make_decision_artifact("m3"))
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore([task]), retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert [msg["content"] for msg in messages] == ["早期摘要", "m2", "m3"]


# ------------------------------------------------------------- live == rebuild


class _RecordingQueue:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


class _DemoArgs(BaseModel):
    value: str


class _DemoTool(FunctionTool):
    name = "demo"
    description = "demo"


async def test_live_messages_match_rebuild():
    """Every runtime.messages append must round-trip through its artifact."""
    queue = _RecordingQueue()
    ctx = SimpleNamespace(
        state=OrchestrationState(),
        task_id="t1",
        context_id="c1",
        queue=queue,
        runtime=SimpleNamespace(messages=[]),
    )
    await record_message(ctx, "你好", role="user")
    await record_message(ctx, "[planner] plan")
    await emit_function_call(
        ctx,
        _DemoTool(),
        _DemoArgs(value="x"),
        FunctionResult(success=True),
        call_id="call-1",
    )

    task = Task(id="t1", context_id="c1")
    for event in queue.events:
        if isinstance(event, TaskArtifactUpdateEvent):
            task.artifacts.append(event.artifact)
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore([task]), retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert messages == ctx.runtime.messages


# ------------------------------------------------------------- compact


async def test_compact_under_threshold_returns_unchanged():
    messages = [OpenAIChatCompletionUserMessage(role="user", content=f"msg{i}") for i in range(5)]
    llm = _StubLLM(token_count=100, context_window=128000)
    builder = HistoryBuilder(llm, _FakeTaskStore([]), compaction_threshold=0.8, retention=10)  # type: ignore[arg-type]

    result, compaction = await builder.compact(messages, "c1")

    assert result is messages
    assert compaction is None


async def test_compact_over_threshold_summarizes_old():
    messages = [OpenAIChatCompletionUserMessage(role="user", content=f"msg{i}") for i in range(15)]
    llm = _StubLLM(summary="旧消息摘要", token_count=200000, context_window=128000)
    builder = HistoryBuilder(llm, _FakeTaskStore([]), compaction_threshold=0.8, retention=5)  # type: ignore[arg-type]

    result, compaction = await builder.compact(messages, "c1")

    assert len(result) == 6
    assert result[0]["role"] == "assistant"
    assert result[0]["content"] == "旧消息摘要"
    assert result[1]["content"] == "msg10"
    assert result[5]["content"] == "msg14"
    assert compaction is not None
    assert compaction.compacted_count == 10
    assert compaction.summary == "旧消息摘要"
    assert len(llm.text_calls) == 1


async def test_compact_keeps_tool_pair_intact():
    messages: list[object] = [
        *[OpenAIChatCompletionUserMessage(role="user", content=f"msg{i}") for i in range(5)],
        OpenAIChatCompletionAssistantMessage(
            role="assistant",
            content=None,
            tool_calls=[
                {
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": "create_plan", "arguments": "{}"},
                }
            ],
        ),
        ChatCompletionToolMessage(role="tool", tool_call_id="call-1", content="{}"),
    ]
    llm = _StubLLM(summary="旧消息摘要", token_count=200000, context_window=128000)
    builder = HistoryBuilder(llm, _FakeTaskStore([]), compaction_threshold=0.8, retention=1)  # type: ignore[arg-type]

    result, compaction = await builder.compact(messages, "c1")  # type: ignore[arg-type]

    assert compaction is not None
    assert [msg["role"] for msg in result] == ["assistant", "assistant", "tool"]
    assert result[1]["tool_calls"][0]["id"] == "call-1"  # type: ignore[index]


async def test_compact_incremental_summary():
    messages = [OpenAIChatCompletionUserMessage(role="user", content=f"msg{i}") for i in range(15)]
    llm = _StubLLM(summary="旧消息摘要", token_count=200000, context_window=128000)
    builder = HistoryBuilder(llm, _FakeTaskStore([]), compaction_threshold=0.8, retention=5)  # type: ignore[arg-type]

    result1, comp1 = await builder.compact(messages, "c1")
    assert comp1 is not None
    assert comp1.compacted_count == 10

    messages2 = [
        *result1,
        *[OpenAIChatCompletionUserMessage(role="user", content=f"new{i}") for i in range(5)],
    ]
    llm._summary = "增量摘要"
    result2, comp2 = await builder.compact(messages2, "c1")

    assert comp2 is not None
    assert comp2.compacted_count == 15  # 10 + 5 new messages
    assert result2[0]["content"] == "增量摘要"
    assert len(llm.text_calls) == 2
    assert "Previous summary" in llm.text_calls[1]["user"]
    assert "旧消息摘要" in llm.text_calls[1]["user"]


async def test_compact_not_enough_to_split():
    messages = [OpenAIChatCompletionUserMessage(role="user", content=f"msg{i}") for i in range(3)]
    llm = _StubLLM(token_count=200000, context_window=128000)
    builder = HistoryBuilder(llm, _FakeTaskStore([]), compaction_threshold=0.8, retention=5)  # type: ignore[arg-type]

    result, compaction = await builder.compact(messages, "c1")

    assert result is messages
    assert compaction is None
