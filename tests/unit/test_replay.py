from a2a.types import Artifact, Part, Task, TaskState, TaskStatus
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict

from choirworks.api.replay import synthesize_replay_events


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

    events = synthesize_replay_events([task], "c1", context=None)

    updates = [e for e in events if "artifactUpdate" in e]
    assert len(updates) == 1
    update = updates[0]["artifactUpdate"]
    assert update["metadata"] == {"node_id": "n1", "agent_name": "product-manager"}
    assert update["lastChunk"] is True


def test_replay_artifact_without_metadata_stays_bare() -> None:
    task = _task_with_artifact({})

    events = synthesize_replay_events([task], "c1", context=None)

    update = [e for e in events if "artifactUpdate" in e][0]["artifactUpdate"]
    assert update.get("metadata") in (None, {})
