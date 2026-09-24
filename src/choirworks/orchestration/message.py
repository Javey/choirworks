# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_graph.py、utils/_graph_validation.py
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal

import structlog
from a2a.helpers import new_task
from a2a.server.agent_execution import RequestContext
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types.a2a_pb2 import TaskState

from choirworks.a2a.room import RoomOptions, room_options
from choirworks.a2a.wire import QuestionResponse, parse_question_response
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.derived import spawn_followup_node
from choirworks.orchestration.events import (
    emit_intervention_rejected,
    emit_pending_questions,
    emit_state_delta,
)
from choirworks.orchestration.graph import Edge, Flow, FlowOutcome
from choirworks.orchestration.intervention import answer_intervention
from choirworks.orchestration.planning import plan_and_launch
from choirworks.orchestration.recover import is_recover_request, recover_session
from choirworks.orchestration.runner import start_runner
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    NodeState,
    NodeStatus,
    active_nodes,
    blocked_nodes,
    enqueue,
    has_pending_work,
    pending_interventions,
)
from choirworks.orchestration.transitions import apply_transition

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class MessagePayload:
    context: RequestContext
    text: str = ""
    room: RoomOptions = field(default_factory=RoomOptions)
    updater: TaskUpdater | None = None
    responses: list[QuestionResponse] = field(default_factory=list)
    malformed: str | None = None
    needs_runner: bool = False
    quote_id: str | None = None
    target: NodeState | None = None


async def _prepare_inbound(
    ctx: OrchestrationContext, payload: MessagePayload
) -> Literal["recover", "ready"]:
    if is_recover_request(payload.context):
        logger.info("execute recover", task_id=ctx.task_id, context_id=ctx.context_id)
        return "recover"
    context = payload.context
    payload.text = (context.get_user_input() or "").strip()
    if context.message is not None:
        try:
            payload.responses = parse_question_response(context.message)
        except ValueError as exc:
            payload.malformed = str(exc)
    if context.current_task is None:
        initial_task = new_task(
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            state=TaskState.TASK_STATE_SUBMITTED,
            history=[context.message] if context.message else None,
        )
        await ctx.queue.enqueue_event(initial_task)
    payload.updater = TaskUpdater(ctx.queue, ctx.task_id, ctx.context_id)
    room = room_options(context.message)
    mentions = list(room.get("mentions") or [])
    for name in re.findall(r"@([A-Za-z0-9_-]+)", payload.text):
        if name not in mentions:
            mentions.append(name)
    if mentions:
        room["mentions"] = mentions
    payload.room = room
    return "ready"


async def _do_recover(ctx: OrchestrationContext, _payload: MessagePayload) -> FlowOutcome:
    await recover_session(ctx)
    return FlowOutcome.END


async def _do_reject(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    logger.info(
        "execute route=intervention_malformed", task_id=ctx.task_id, context_id=ctx.context_id
    )
    await emit_intervention_rejected(ctx, "", payload.malformed or "")
    return FlowOutcome.END


async def _do_answers(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    logger.info(
        "execute route=intervention_answers",
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        answers=len(payload.responses),
    )
    resolved = False
    for response in payload.responses:
        if await answer_intervention(
            ctx, intervention_id=response.intervention_id, answer=response.answer
        ):
            resolved = True
    if resolved:
        if pending_interventions(ctx.state):
            await emit_pending_questions(ctx)
        ctx.runtime.wake.set()
        start_runner(ctx)
    return FlowOutcome.END


async def _do_reemit_questions(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    logger.info(
        "execute route=intervention_unanswered",
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        text_len=len(payload.text),
    )
    await emit_pending_questions(ctx)
    return FlowOutcome.END


async def _do_complete(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    logger.info("execute route=empty_complete", task_id=ctx.task_id, context_id=ctx.context_id)
    updater = payload.updater
    assert updater is not None
    await updater.complete()
    ctx.sessions.evict_session(ctx.context_id)
    return FlowOutcome.END


async def _do_plan_and_launch(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    logger.info(
        "execute route=plan_and_launch",
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        text_len=len(payload.text),
    )
    updater = payload.updater
    assert updater is not None
    await updater.start_work()
    await plan_and_launch(ctx, payload.text, room=payload.room)
    return FlowOutcome.END


def _finish(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    if payload.needs_runner:
        start_runner(ctx)
    return FlowOutcome.END


async def _do_noop(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    return _finish(ctx, payload)


async def _do_enqueue(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    node = payload.target
    if node is None:
        return _finish(ctx, payload)
    logger.info(
        "execute route=enqueue", task_id=ctx.task_id, context_id=ctx.context_id, node_id=node.id
    )
    _ = enqueue(ctx.state, node.id, payload.text, sender="user", quote_id=payload.quote_id)
    await ctx.sessions.persist(ctx)
    return _finish(ctx, payload)


async def _do_spawn_followup(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    node = payload.target
    if node is None:
        return _finish(ctx, payload)
    logger.info(
        "execute route=followup", task_id=ctx.task_id, context_id=ctx.context_id, node_id=node.id
    )
    await spawn_followup_node(ctx, payload.text, node)
    return _finish(ctx, payload)


async def _do_interrupt(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    node = payload.target
    if node is None:
        return _finish(ctx, payload)
    logger.info(
        "execute route=interrupt", task_id=ctx.task_id, context_id=ctx.context_id, node_id=node.id
    )
    if node.a2a_task_id:
        await ctx.remote.cancel_task(node.agent_url, node.a2a_task_id)
    apply_transition(node, NodeStatus.CANCELED)
    invalidated = blocked_nodes(ctx.state)
    for blocked in invalidated:
        apply_transition(blocked, NodeStatus.INVALIDATED)
    await emit_state_delta(
        ctx,
        nodes={
            node.id: {"status": NodeStatus.CANCELED, "agent_name": node.agent_name},
            **{b.id: {"status": NodeStatus.INVALIDATED} for b in invalidated},
        },
    )
    await spawn_followup_node(ctx, payload.text, node, deps=[])
    return _finish(ctx, payload)


async def _do_new_plan(ctx: OrchestrationContext, payload: MessagePayload) -> FlowOutcome:
    logger.info("execute route=new_plan", task_id=ctx.task_id, context_id=ctx.context_id)
    await plan_and_launch(ctx, payload.text)
    return _finish(ctx, payload)


async def _classify_inbound(
    ctx: OrchestrationContext, payload: MessagePayload
) -> Literal[
    "malformed",
    "answers",
    "unanswered_pending",
    "room",
    "empty",
    "plan",
]:
    if payload.malformed is not None:
        return "malformed"
    if payload.responses:
        return "answers"
    if pending_interventions(ctx.state):
        return "unanswered_pending"
    runner = ctx.runtime.runner
    if runner is not None and not runner.done():
        return "room"
    if has_pending_work(ctx.state):
        payload.needs_runner = True
        return "room"
    if not payload.text:
        return "empty"
    return "plan"


_QUOTED = Literal["quote_active", "quote_completed", "interrupt", "target", "new_plan", "noop"]


async def _classify_room(ctx: OrchestrationContext, payload: MessagePayload) -> _QUOTED:
    if not payload.text:
        return "noop"
    state = ctx.state
    active = active_nodes(state)
    quote_id = payload.room.get("quote_id")
    interrupt = bool(payload.room.get("interrupt"))
    if quote_id and not interrupt:
        quoted = next(
            (
                node
                for node in state.nodes.values()
                if node.id == quote_id or f"{ctx.task_id}:{node.id}" == quote_id
            ),
            None,
        )
        if quoted is not None and quoted.status in ACTIVE_NODE_STATUSES:
            payload.quote_id = str(quote_id)
            payload.target = quoted
            return "quote_active"
        if quoted is not None and quoted.status == NodeStatus.COMPLETED:
            payload.target = quoted
            return "quote_completed"
    if interrupt and active:
        payload.target = active[0]
        return "interrupt"
    target = (
        active[0]
        if active
        else next(
            (n for n in state.nodes.values() if n.status in {NodeStatus.PENDING, NodeStatus.READY}),
            None,
        )
    )
    if target is not None:
        payload.quote_id = str(quote_id) if quote_id else None
        payload.target = target
        return "target"
    return "new_plan"


message_flow: Flow[MessagePayload] = Flow(
    name="message_flow",
    start="prepare_inbound",
    handlers={
        "prepare_inbound": _prepare_inbound,
        "classify_inbound": _classify_inbound,
        "do_recover": _do_recover,
        "do_reject": _do_reject,
        "do_answers": _do_answers,
        "do_reemit_questions": _do_reemit_questions,
        "classify_room": _classify_room,
        "do_complete": _do_complete,
        "do_plan_and_launch": _do_plan_and_launch,
        "do_noop": _do_noop,
        "do_enqueue": _do_enqueue,
        "do_spawn_followup": _do_spawn_followup,
        "do_interrupt": _do_interrupt,
        "do_new_plan": _do_new_plan,
    },
    edges=(
        Edge("prepare_inbound", "do_recover", frozenset({"recover"})),
        Edge("prepare_inbound", "classify_inbound", frozenset({"ready"})),
        Edge("classify_inbound", "do_reject", frozenset({"malformed"})),
        Edge("classify_inbound", "do_answers", frozenset({"answers"})),
        Edge("classify_inbound", "do_reemit_questions", frozenset({"unanswered_pending"})),
        Edge("classify_inbound", "classify_room", frozenset({"room"})),
        Edge("classify_inbound", "do_complete", frozenset({"empty"})),
        Edge("classify_inbound", "do_plan_and_launch", frozenset({"plan"})),
        Edge("classify_room", "do_noop", frozenset({"noop"})),
        Edge("classify_room", "do_enqueue", frozenset({"quote_active", "target"})),
        Edge("classify_room", "do_spawn_followup", frozenset({"quote_completed"})),
        Edge("classify_room", "do_interrupt", frozenset({"interrupt"})),
        Edge("classify_room", "do_new_plan", frozenset({"new_plan"})),
    ),
)
message_flow.validate()
