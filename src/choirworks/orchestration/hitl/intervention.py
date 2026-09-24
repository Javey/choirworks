from __future__ import annotations

import uuid

import structlog
from a2a.types.a2a_pb2 import Message, Part, Role, TaskState
from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ValidationError

from choirworks.a2a.wire import data_part
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import (
    emit_event,
    emit_intervention_rejected,
    emit_state_delta,
)
from choirworks.orchestration.execution.remote_caller import cancel_remote_task
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    Intervention,
    InterventionKind,
    InterventionStatus,
    NodeState,
    NodeStatus,
    QuestionType,
    blocked_nodes,
    pending_interventions,
)
from choirworks.orchestration.transitions import apply_transition

logger = structlog.get_logger(__name__)

QUESTION_PART = "question"
QUESTION_RESPONSE_PART = "question_response"


class QuestionResponse(BaseModel):
    """A user's answer to one pending question."""

    intervention_id: str
    answer: str | list[str] | bool


def parse_question_response(message: Message) -> list[QuestionResponse]:
    """Extract ``question_response`` data parts from a user message.

    Returns an empty list when the message carries no such part; raises
    ``ValueError`` when a part is malformed (missing id / bad answer type).
    """
    responses: list[QuestionResponse] = []
    for part in message.parts:
        if part.WhichOneof("content") != "data":
            continue
        kind_field = part.metadata.fields.get("cw_type")
        if kind_field is None or kind_field.string_value != QUESTION_RESPONSE_PART:
            continue
        payload = MessageToDict(part.data, preserving_proto_field_name=True)
        try:
            responses.append(QuestionResponse.model_validate(payload))
        except ValidationError as exc:
            raise ValueError(f"malformed question_response: {payload!r}") from exc
    return responses


def build_questions_message(ctx: OrchestrationContext, pending: list[Intervention]) -> Message:
    """Aggregate pending questions into one agent message (text + data parts)."""
    parts: list[Part] = []
    for intervention in pending:
        parts.append(Part(text=intervention.question))
        parts.append(
            data_part(
                {
                    "intervention_id": intervention.id,
                    "node_id": intervention.node_id,
                    "requester": intervention.requester,
                    "kind": intervention.kind,
                    "question_type": intervention.question_type,
                    "options": list(intervention.options),
                    "multi": intervention.multi,
                    "question": intervention.question,
                },
                {"cw_type": QUESTION_PART},
            )
        )
    return Message(
        role=Role.ROLE_AGENT,
        message_id=uuid.uuid4().hex,
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        parts=parts,
    )


async def emit_pending_questions(ctx: OrchestrationContext) -> None:
    """Emit the aggregated input-required question message (set changed)."""
    pending = pending_interventions(ctx.state)
    if not pending:
        return
    await emit_event(
        ctx,
        "questions",
        TaskState.TASK_STATE_INPUT_REQUIRED,
        message=build_questions_message(ctx, pending),
    )


def render_answer(intervention: Intervention) -> str:
    """Render a typed answer as the continuation text sent to the node."""
    answer = intervention.answer
    if intervention.question_type == QuestionType.SELECT and isinstance(answer, list):
        return "、".join(answer)
    if intervention.question_type == QuestionType.CONFIRM:
        return "确认" if answer is True else "取消"
    return answer if isinstance(answer, str) else ""


def _validate_answer(intervention: Intervention, answer: str | list[str] | bool) -> str | None:
    if intervention.question_type == QuestionType.SELECT:
        options = set(intervention.options)
        if intervention.multi:
            if not isinstance(answer, list) or not answer:
                return "multi-select answer must be a non-empty list"
            if not set(answer) <= options:
                return "answer contains options outside the allowed set"
        elif not isinstance(answer, str) or answer not in options:
            return "answer must be one of the allowed options"
    elif intervention.question_type == QuestionType.CONFIRM:
        if not isinstance(answer, bool):
            return "confirm answer must be a boolean"
    elif not isinstance(answer, str) or not answer.strip():
        return "input answer must be a non-empty string"
    return None


async def _expire(ctx: OrchestrationContext, intervention: Intervention) -> bool:
    intervention.status = InterventionStatus.EXPIRED
    await ctx.sessions.persist(ctx)
    await emit_state_delta(
        ctx,
        interventions={
            intervention.id: {
                "status": "expired",
                "node_id": intervention.node_id,
                "kind": intervention.kind,
            },
        },
    )
    return False


async def answer_intervention(
    ctx: OrchestrationContext,
    *,
    intervention_id: str,
    answer: str | list[str] | bool,
) -> bool:
    """Resolve one pending intervention by id with a typed answer.

    Returns True when the intervention was resolved and the runner should be
    woken.  Unknown / stale ids and ill-typed answers are rejected (the
    intervention expires when its target is gone), never resurrecting nodes.
    """
    state = ctx.state
    intervention = state.interventions.get(intervention_id)
    if intervention is None or intervention.status != InterventionStatus.PENDING:
        await emit_intervention_rejected(ctx, intervention_id, "unknown or resolved intervention")
        return False
    error = _validate_answer(intervention, answer)
    if error is not None:
        await emit_intervention_rejected(ctx, intervention_id, error)
        return False
    logger.info(
        "answer_intervention",
        id=intervention.id,
        kind=intervention.kind,
        question_type=intervention.question_type,
    )
    if intervention.kind == InterventionKind.CONFIRM_CANCEL:
        target = state.nodes.get(intervention.target_node_id or "")
        if target is None or target.status not in ACTIVE_NODE_STATUSES:
            return await _expire(ctx, intervention)
        intervention.status = InterventionStatus.RESOLVED
        intervention.answer = answer
        intervention.responder = "human"
        if answer is True:
            await cancel_node(ctx, target)
        await ctx.sessions.persist(ctx)
        await emit_state_delta(
            ctx,
            interventions={
                intervention.id: {
                    "status": "resolved",
                    "node_id": intervention.node_id,
                    "kind": "confirm_cancel",
                    "responder": "human",
                    "answer": answer,
                },
            },
        )
        return True
    node = state.nodes.get(intervention.node_id)
    if node is None or node.status != NodeStatus.INPUT_REQUIRED:
        return await _expire(ctx, intervention)
    intervention.status = InterventionStatus.RESOLVED
    intervention.answer = answer
    intervention.responder = "human"
    node.answer_text = render_answer(intervention)
    apply_transition(node, NodeStatus.READY)
    await ctx.sessions.persist(ctx)
    await emit_state_delta(
        ctx,
        nodes={node.id: {"status": NodeStatus.READY}},
        interventions={
            intervention.id: {
                "status": "resolved",
                "node_id": intervention.node_id,
                "kind": intervention.kind,
                "responder": "human",
                "answer": answer,
            },
        },
    )
    return True


async def cancel_node(
    ctx: OrchestrationContext,
    node: NodeState,
) -> None:
    logger.info(
        "cancel_node",
        node=node.id,
        invalidated=[n.id for n in blocked_nodes(ctx.state)],
    )
    state = ctx.state
    if node.a2a_task_id and node.agent_url:
        await cancel_remote_task(ctx, node.agent_url, node.a2a_task_id)
    apply_transition(node, NodeStatus.CANCELED)
    node.a2a_task_id = None
    invalidated = blocked_nodes(state)
    for blocked in invalidated:
        apply_transition(blocked, NodeStatus.INVALIDATED)
    await emit_state_delta(
        ctx,
        nodes={
            node.id: {"status": NodeStatus.CANCELED, "agent_name": node.agent_name},
            **{b.id: {"status": NodeStatus.INVALIDATED} for b in invalidated},
        },
    )
    await ctx.sessions.persist(ctx)
