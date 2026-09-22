from a2a.types import Artifact, Part, Task, TaskState, TaskStatus
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict

from choirworks.api.replay import synthesize_replay_events
from choirworks.orchestration.state import (
    Intervention,
    Member,
    NodeState,
    OrchestrationState,
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


def test_replay_artifact_keeps_node_metadata_on_event() -> None:
    task = _task_with_artifact({"node_id": "n1", "agent_name": "product-manager"})

    events = synthesize_replay_events([task], "c1", state=None)

    updates = [e for e in events if "artifactUpdate" in e]
    assert len(updates) == 1
    update = updates[0]["artifactUpdate"]
    assert update["metadata"] == {"node_id": "n1", "agent_name": "product-manager"}
    assert update["lastChunk"] is True


def test_replay_artifact_without_metadata_stays_bare() -> None:
    task = _task_with_artifact({})

    events = synthesize_replay_events([task], "c1", state=None)

    update = [e for e in events if "artifactUpdate" in e][0]["artifactUpdate"]
    assert update.get("metadata") in (None, {})


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
        agent_url="http://agent",
        status="completed",
        output="调研结果",
    )
    state.members["researcher"] = Member(
        name="researcher", url="http://agent", reason="plan"
    )
    state.interventions["i1"] = Intervention(
        id="i1", node_id="n1", question="需要确认吗", status="resolved"
    )
    task = _task_with_artifact({})

    events = synthesize_replay_events([task], "c1", state)

    updates = [e for e in events if "statusUpdate" in e]
    assert len(updates) == 1
    metadata = updates[0]["statusUpdate"]["metadata"]
    assert metadata["kind"] == "state_delta"
    assert metadata["nodes"]["n1"]["status"] == "completed"
    assert metadata["nodes"]["n1"]["output"] == "调研结果"
    assert metadata["members"][0]["name"] == "researcher"
    assert metadata["interventions"]["i1"]["status"] == "resolved"
