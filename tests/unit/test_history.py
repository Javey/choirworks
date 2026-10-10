from __future__ import annotations

from a2a.types.a2a_pb2 import Artifact, Message, Part, Role, Task
from google.protobuf import struct_pb2

from choirworks.core.events import CwType
from choirworks.orchestration.history import HistoryBuilder


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


async def test_rebuild_reads_task_history():
    tasks = [
        _make_task("t2", "c1", [(Role.ROLE_AGENT, "[translator] 翻译完成")]),
        _make_task("t1", "c1", [(Role.ROLE_USER, "你好"), (Role.ROLE_AGENT, "[planner] plan")]),
    ]
    store = _FakeTaskStore(tasks)
    llm = _StubLLM()
    builder = HistoryBuilder(llm, store, retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert len(messages) == 3
    assert messages[0]["role"] == "user"
    assert messages[0]["content"] == "你好"
    assert messages[1]["role"] == "assistant"
    assert messages[1]["content"] == "[planner] plan"
    assert messages[2]["content"] == "[translator] 翻译完成"


async def test_rebuild_excludes_task():
    tasks = [
        _make_task("t2", "c1", [(Role.ROLE_USER, "排除我")]),
        _make_task("t1", "c1", [(Role.ROLE_USER, "你好")]),
    ]
    store = _FakeTaskStore(tasks)
    llm = _StubLLM()
    builder = HistoryBuilder(llm, store, retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1", exclude_task_id="t2")

    assert len(messages) == 1
    assert messages[0]["content"] == "你好"


async def test_rebuild_empty_context():
    builder = HistoryBuilder(_StubLLM(), _FakeTaskStore([]), retention=10)  # type: ignore[arg-type]
    messages = await builder.rebuild("")
    assert messages == []


async def test_rebuild_applies_compaction():
    tasks = [
        _make_task("t2", "c1", [(Role.ROLE_AGENT, "[translator] done")]),
        _make_task(
            "t1",
            "c1",
            [
                (Role.ROLE_USER, "你好"),
                (Role.ROLE_AGENT, "[planner] plan"),
                (Role.ROLE_AGENT, "[worker] output1"),
            ],
        ),
    ]
    tasks[1].artifacts.append(_make_compaction_artifact(2, "早期摘要"))
    store = _FakeTaskStore(tasks)
    llm = _StubLLM()
    builder = HistoryBuilder(llm, store, retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert len(messages) == 3
    assert messages[0]["content"] == "早期摘要"
    assert messages[1]["content"] == "[worker] output1"
    assert messages[2]["content"] == "[translator] done"


async def test_rebuild_reads_decision_artifacts():
    task = _make_task("t1", "c1", [(Role.ROLE_USER, "你好")])
    task.artifacts.append(_make_decision_artifact("[planner] plan"))
    task.artifacts.append(_make_decision_artifact("[worker] output", "assistant"))
    store = _FakeTaskStore([task])
    llm = _StubLLM()
    builder = HistoryBuilder(llm, store, retention=10)  # type: ignore[arg-type]

    messages = await builder.rebuild("c1")

    assert len(messages) == 3
    assert messages[0]["content"] == "你好"
    assert messages[1]["content"] == "[planner] plan"
    assert messages[2]["content"] == "[worker] output"


# ------------------------------------------------------------- compact


async def test_compact_under_threshold_returns_unchanged():
    from litellm.types.llms.openai import OpenAIChatCompletionUserMessage

    messages = [OpenAIChatCompletionUserMessage(role="user", content=f"msg{i}") for i in range(5)]
    llm = _StubLLM(token_count=100, context_window=128000)
    builder = HistoryBuilder(llm, _FakeTaskStore([]), compaction_threshold=0.8, retention=10)  # type: ignore[arg-type]

    result, compaction = await builder.compact(messages, "c1")

    assert result is messages
    assert compaction is None


async def test_compact_over_threshold_summarizes_old():
    from litellm.types.llms.openai import OpenAIChatCompletionUserMessage

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


async def test_compact_incremental_summary():
    from litellm.types.llms.openai import OpenAIChatCompletionUserMessage

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
    from litellm.types.llms.openai import OpenAIChatCompletionUserMessage

    messages = [OpenAIChatCompletionUserMessage(role="user", content=f"msg{i}") for i in range(3)]
    llm = _StubLLM(token_count=200000, context_window=128000)
    builder = HistoryBuilder(llm, _FakeTaskStore([]), compaction_threshold=0.8, retention=5)  # type: ignore[arg-type]

    result, compaction = await builder.compact(messages, "c1")

    assert result is messages
    assert compaction is None
