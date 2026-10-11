from a2a.types import Artifact, Message, Part, Role, Task, TaskState, TaskStatus
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict

from choirworks.api.replay import synthesize_replay_events
from choirworks.orchestration.state import (
    Intervention,
    Member,
    NodeState,
    OrchestrationState,
    QuestionType,
)


def _struct(data: dict[str, object]) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    ParseDict(data, s)
    return s


def _task_with_artifact(metadata: dict[str, object]) -> Task:
    task = Task(
        id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
    )
    task.artifacts.append(
        Artifact(
            artifact_id="a1",
            parts=[Part(text="调研结果")],
            metadata=_struct(metadata),
        )
    )
    return task


def test_replay_artifact_keeps_metadata_on_artifact_only() -> None:
    task = _task_with_artifact({"author": "product-manager", "node_id": "n1"})

    events = synthesize_replay_events([task], "c1", state=None)

    updates = [e for e in events if "artifactUpdate" in e]
    assert len(updates) == 1
    update = updates[0]["artifactUpdate"]
    assert update.get("metadata") in (None, {})
    assert update["artifact"]["metadata"] == {"author": "product-manager", "node_id": "n1"}
    assert update["lastChunk"] is True


def test_replay_artifact_without_metadata_stays_bare() -> None:
    task = _task_with_artifact({})

    events = synthesize_replay_events([task], "c1", state=None)

    update = [e for e in events if "artifactUpdate" in e][0]["artifactUpdate"]
    assert update.get("metadata") in (None, {})


def test_replay_merged_artifact_keeps_part_metadata() -> None:
    part = Part(text="你好")
    part.metadata.CopyFrom(_struct({"lang": "zh"}))
    task = Task(
        id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
    )
    task.artifacts.append(Artifact(artifact_id="a1", parts=[part]))

    events = synthesize_replay_events([task], "c1", state=None)

    update = [e for e in events if "artifactUpdate" in e][0]["artifactUpdate"]
    assert update["artifact"]["parts"][0]["metadata"] == {"lang": "zh"}


def test_replay_snapshot_drops_history_and_artifacts() -> None:
    task = Task(
        id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        history=[Message(message_id="m1", role=Role.ROLE_USER, parts=[Part(text="开发网站")])],
    )
    task.artifacts.append(Artifact(artifact_id="a1", parts=[Part(text="回复")]))

    events = synthesize_replay_events([task], "c1", state=None)

    snapshot = events[0]["task"]
    assert snapshot.get("history") in (None, [])
    assert snapshot.get("artifacts") in (None, [])


def test_replay_user_history_becomes_positioned_message() -> None:
    part = Part(text="开发网站")
    part.metadata.CopyFrom(_struct({"cw_type": "history", "role": "user"}))
    task = Task(
        id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        history=[Message(message_id="m1", role=Role.ROLE_USER, parts=[Part(text="开发网站")])],
    )
    task.artifacts.append(Artifact(artifact_id="h1", parts=[part]))
    task.artifacts.append(Artifact(artifact_id="a1", parts=[Part(text="回复")]))

    events = synthesize_replay_events([task], "c1", state=None)

    assert [next(iter(event)) for event in events] == [
        "task",
        "message",
        "artifactUpdate",
    ]
    message = events[1]["message"]
    assert message["messageId"] == "m1"
    assert message["role"] == Role.ROLE_USER
    assert message["parts"][0]["text"] == "开发网站"


def test_replay_question_artifact_becomes_positioned_pending_delta() -> None:
    question = Part(text="请确认是否采用该方案？")
    question.metadata.CopyFrom(_struct({"cw_type": "history", "role": "assistant"}))
    task = Task(
        id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
    )
    task.artifacts.append(Artifact(artifact_id="h1", parts=[question]))
    state = OrchestrationState()
    state.interventions["i1"] = Intervention(
        id="i1",
        node_id="n1",
        question="请确认是否采用该方案？",
        requester="code-reviewer",
        question_type=QuestionType.CONFIRM,
        status="resolved",
        answer=True,
        responder="human",
    )

    events = synthesize_replay_events([task], "c1", state)

    assert [next(iter(event)) for event in events] == ["task", "statusUpdate", "statusUpdate"]
    delta = events[1]["statusUpdate"]["metadata"]["cw_delta"]["interventions"]["i1"]
    assert delta["status"] == "pending"
    assert delta["question"] == "请确认是否采用该方案？"
    assert delta["question_type"] == "confirm"
    assert "answer" not in delta


def test_replay_answer_artifact_resolves_intervention_in_place() -> None:
    def _history(text: str, role: str) -> Artifact:
        part = Part(text=text)
        part.metadata.CopyFrom(_struct({"cw_type": "history", "role": role}))
        return Artifact(artifact_id=f"h-{text}", parts=[part])

    task = Task(
        id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
    )
    task.artifacts.append(_history("请确认是否采用该方案？", "assistant"))
    task.artifacts.append(_history("确认", "user"))
    state = OrchestrationState()
    state.interventions["i1"] = Intervention(
        id="i1",
        node_id="n1",
        question="请确认是否采用该方案？",
        question_type=QuestionType.CONFIRM,
        status="resolved",
        answer=True,
        responder="human",
    )

    events = synthesize_replay_events([task], "c1", state)

    assert [next(iter(event)) for event in events] == [
        "task",
        "statusUpdate",
        "statusUpdate",
        "statusUpdate",
    ]
    pending = events[1]["statusUpdate"]["metadata"]["cw_delta"]["interventions"]["i1"]
    assert pending["status"] == "pending"
    resolved = events[2]["statusUpdate"]["metadata"]["cw_delta"]["interventions"]["i1"]
    assert resolved["status"] == "resolved"
    assert resolved["answer"] is True
    final = events[3]["statusUpdate"]["metadata"]["cw_delta"]["interventions"]["i1"]
    assert final["status"] == "resolved"


def test_replay_unknown_assistant_history_falls_back_to_artifact() -> None:
    part = Part(text="无人认领的问题")
    part.metadata.CopyFrom(_struct({"cw_type": "history", "role": "assistant"}))
    task = Task(
        id="t1",
        context_id="c1",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
    )
    task.artifacts.append(Artifact(artifact_id="h1", parts=[part]))

    events = synthesize_replay_events([task], "c1", state=None)

    assert [next(iter(event)) for event in events] == ["task", "artifactUpdate"]
    update = events[1]["artifactUpdate"]
    assert update["artifact"]["parts"][0]["metadata"] == {
        "cw_type": "history",
        "role": "assistant",
    }


def test_replay_interleaves_task_events_with_their_artifacts() -> None:
    def _task(task_id: str, artifact_text: str) -> Task:
        task = Task(
            id=task_id,
            context_id="c1",
            status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        )
        task.artifacts.append(
            Artifact(artifact_id=f"a-{task_id}", parts=[Part(text=artifact_text)])
        )
        return task

    tasks = [_task("t1", "第一轮回复"), _task("t2", "第二轮回复")]

    events = synthesize_replay_events(tasks, "c1", state=None)

    shapes = ["task" if "task" in event else "artifactUpdate" for event in events]
    assert shapes == ["task", "artifactUpdate", "task", "artifactUpdate"]


def test_replay_state_snapshot_becomes_state_delta() -> None:
    state = OrchestrationState()
    state.nodes["n1"] = NodeState(
        id="n1",
        name="调研",
        agent_name="researcher",
        status="completed",
        output="调研结果",
    )
    state.members["researcher"] = Member(name="researcher", reason="plan")
    state.interventions["i1"] = Intervention(
        id="i1",
        node_id="n1",
        question="需要确认吗",
        status="resolved",
        responder="human",
    )
    task = _task_with_artifact({})

    events = synthesize_replay_events([task], "c1", state)

    updates = [e for e in events if "statusUpdate" in e]
    assert len(updates) == 1
    metadata = updates[0]["statusUpdate"]["metadata"]
    assert metadata["cw_delta"]["nodes"]["n1"]["status"] == "completed"
    assert metadata["cw_delta"]["nodes"]["n1"]["output"] == "调研结果"
    assert metadata["cw_delta"]["members"][0]["agent_name"] == "researcher"
    assert metadata["cw_delta"]["interventions"]["i1"]["status"] == "resolved"
    assert metadata["cw_delta"]["interventions"]["i1"]["responder"] == "human"
