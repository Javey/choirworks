from __future__ import annotations

from a2a.types.a2a_pb2 import (
    Artifact,
    Part,
    StreamResponse,
    Task,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatusUpdateEvent,
)
from google.protobuf.json_format import MessageToDict

from choirworks.core.events import (
    FUNCTION_CALL,
    THOUGHT,
    state_delta_event,
)
from choirworks.orchestration.state import (
    Intervention,
    InterventionDelta,
    MemberDict,
    NodeDelta,
    OrchestrationState,
)


def _intervention_delta(intervention: Intervention) -> InterventionDelta:
    delta: InterventionDelta = {
        "status": intervention.status,
        "node_id": intervention.node_id,
        "kind": intervention.kind,
        "question": intervention.question,
        "responder": intervention.responder or "",
        "question_type": intervention.question_type,
        "options": list(intervention.options),
        "multi": intervention.multi,
        "requester": intervention.requester,
    }
    if intervention.answer is not None:
        delta["answer"] = intervention.answer
    return delta


def _state_delta_event(
    state: OrchestrationState,
    context_id: str,
    task_id: str,
    task_state: TaskState,
) -> TaskStatusUpdateEvent:
    nodes: dict[str, NodeDelta] = {
        node_id: {
            "status": node.status,
            "output": node.output or "",
            "error": node.error or "",
            "name": node.name,
            "agent_name": node.agent_name,
        }
        for node_id, node in state.nodes.items()
        if node_id
    }

    members: list[MemberDict] = [member.to_dict() for member in state.members.values()]

    interventions: dict[str, InterventionDelta] = {
        intervention.id: _intervention_delta(intervention)
        for intervention in state.interventions.values()
        if intervention.id
    }

    return state_delta_event(
        task_id,
        context_id,
        {
            "nodes": nodes,
            "members": members,
            "interventions": interventions,
        },
        state=task_state,
    )


def _artifact_update(
    task_id: str,
    context_id: str,
    artifact: Artifact,
) -> TaskArtifactUpdateEvent:
    return TaskArtifactUpdateEvent(
        task_id=task_id,
        context_id=context_id,
        artifact=artifact,
        append=False,
        last_chunk=True,
    )


def synthesize_replay_events(
    tasks: list[Task],
    context_id: str,
    state: OrchestrationState | None,
) -> list[dict[str, object]]:
    if not tasks:
        return []

    events: list[StreamResponse] = []

    for task in tasks:
        task_copy = Task()
        task_copy.CopyFrom(task)
        del task_copy.artifacts[:]
        events.append(StreamResponse(task=task_copy))

        for art in task.artifacts:
            parts = list(art.parts)
            if not parts:
                continue
            kind_field = parts[0].metadata.fields.get("cw_type")
            kind = kind_field.string_value if kind_field is not None else ""
            is_verbatim = kind in {THOUGHT, FUNCTION_CALL}

            if is_verbatim:
                events.append(
                    StreamResponse(
                        artifact_update=_artifact_update(
                            task.id,
                            context_id,
                            art,
                        )
                    )
                )
            else:
                merged_text = "".join(p.text for p in parts if p.text)
                if merged_text:
                    merged = Artifact(
                        artifact_id=art.artifact_id,
                        name=art.name,
                        parts=[Part(text=merged_text)],
                        metadata=art.metadata,
                    )
                    events.append(
                        StreamResponse(
                            artifact_update=_artifact_update(
                                task.id,
                                context_id,
                                merged,
                            )
                        )
                    )

    if state is not None and tasks:
        last_task = tasks[-1]
        task_state = last_task.status.state
        events.append(
            StreamResponse(
                status_update=_state_delta_event(
                    state,
                    context_id,
                    last_task.id,
                    task_state,
                )
            )
        )

    return [MessageToDict(e, use_integers_for_enums=True) for e in events]
