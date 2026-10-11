from __future__ import annotations

import uuid

from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    Role,
    StreamResponse,
    Task,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatusUpdateEvent,
)
from google.protobuf.json_format import MessageToDict

from choirworks.core.events import (
    CwType,
    state_delta_event,
)
from choirworks.orchestration.state import (
    Intervention,
    InterventionDelta,
    InterventionStatus,
    MemberDict,
    NodeDelta,
    OrchestrationState,
    render_answer,
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


def _message_event(
    task_id: str,
    context_id: str,
    message_id: str,
    text: str,
) -> StreamResponse:
    """A user room message, positioned where its history artifact was recorded."""
    return StreamResponse(
        message=Message(
            message_id=message_id,
            role=Role.ROLE_USER,
            task_id=task_id,
            context_id=context_id,
            parts=[Part(text=text)],
        )
    )


def _intervention_delta_event(
    task: Task,
    context_id: str,
    intervention: Intervention,
    *,
    status: str = "",
) -> StreamResponse:
    """One intervention transition, positioned at its history artifact.

    Reusing ``cw_delta`` keeps the frontend's existing delta handling: the
    card is created/updated at this exact seq instead of at the final
    full-state delta.
    """
    delta = _intervention_delta(intervention)
    if status:
        delta["status"] = status
        delta.pop("answer", None)
    return StreamResponse(
        status_update=state_delta_event(
            task.id,
            context_id,
            {"interventions": {intervention.id: delta}},
            state=task.status.state,
        )
    )


def _find_question(
    interventions: list[Intervention],
    claimed: set[str],
    text: str,
) -> Intervention | None:
    for intervention in interventions:
        if intervention.id in claimed or intervention.question != text:
            continue
        return intervention
    return None


def _find_answer(
    interventions: list[Intervention],
    claimed: set[str],
    answered: set[str],
    text: str,
) -> Intervention | None:
    for intervention in interventions:
        if intervention.id not in claimed or intervention.id in answered:
            continue
        if intervention.status != InterventionStatus.RESOLVED:
            continue
        if render_answer(intervention) != text:
            continue
        return intervention
    return None


def synthesize_replay_events(
    tasks: list[Task],
    context_id: str,
    state: OrchestrationState | None,
) -> list[dict[str, object]]:
    if not tasks:
        return []

    events: list[StreamResponse] = []
    interventions = (
        sorted(state.interventions.values(), key=lambda item: item.created_at)
        if state is not None
        else []
    )
    claimed: set[str] = set()
    answered: set[str] = set()

    for task in tasks:
        task_copy = Task()
        task_copy.CopyFrom(task)
        del task_copy.artifacts[:]
        # History is a second, unordered channel: artifacts already carry
        # every UI-visible message at its true position. Seeding history
        # would pin messages/questions to the snapshot seq instead.
        del task_copy.history[:]
        events.append(StreamResponse(task=task_copy))

        pending_user_ids: dict[str, list[str]] = {}
        for message in task.history:
            if message.role != Role.ROLE_USER:
                continue
            text = "".join(part.text for part in message.parts if part.text)
            pending_user_ids.setdefault(text, []).append(message.message_id)

        for art in task.artifacts:
            parts = list(art.parts)
            if not parts:
                continue
            kind_field = parts[0].metadata.fields.get("cw_type")
            kind = kind_field.string_value if kind_field is not None else ""

            if kind == CwType.HISTORY:
                handled = _replay_history_artifact(
                    events,
                    task,
                    context_id,
                    art,
                    interventions,
                    claimed,
                    answered,
                    pending_user_ids,
                )
                if handled:
                    continue

            is_verbatim = kind in {CwType.THOUGHT, CwType.FUNCTION_CALL}

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
                        parts=[Part(text=merged_text, metadata=parts[0].metadata)],
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


def _replay_history_artifact(
    events: list[StreamResponse],
    task: Task,
    context_id: str,
    art: Artifact,
    interventions: list[Intervention],
    claimed: set[str],
    answered: set[str],
    pending_user_ids: dict[str, list[str]],
) -> bool:
    """Emit positioned events for one ``cw_type=history`` artifact.

    Returns True when the artifact was translated (no artifact update should
    follow), False to fall back to the generic merged artifact behavior.
    """
    meta = art.parts[0].metadata.fields
    role_field = meta.get("role")
    role = role_field.string_value if role_field is not None else ""
    text = "".join(part.text for part in art.parts if part.text)
    if not text:
        return False

    if role == "user":
        # The human answer lives in the question card ("已答"), not as a
        # bubble; emit only the resolved transition right where it happened.
        answer = _find_answer(interventions, claimed, answered, text)
        if answer is not None:
            answered.add(answer.id)
            events.append(_intervention_delta_event(task, context_id, answer))
            return True
        ids = pending_user_ids.get(text)
        message_id = ids.pop(0) if ids else uuid.uuid4().hex
        events.append(_message_event(task.id, context_id, message_id, text))
        return True

    if role == "assistant":
        question = _find_question(interventions, claimed, text)
        if question is not None:
            claimed.add(question.id)
            events.append(_intervention_delta_event(task, context_id, question, status="pending"))
            return True

    return False
