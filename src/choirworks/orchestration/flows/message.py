# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_graph.py、utils/_graph_validation.py
from __future__ import annotations

import re
from typing import Literal

import structlog
from a2a.helpers import new_task
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types.a2a_pb2 import TaskState

from choirworks.a2a.recovery import is_recover_request, recover_session
from choirworks.a2a.room import room_options
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import (
    emit_intervention_rejected,
    emit_state_delta,
)
from choirworks.orchestration.execution.runner import start_runner
from choirworks.orchestration.flows.engine import Edge, Flow, FlowOutcome
from choirworks.orchestration.helpers import UnknownAgentError, agent_url_for
from choirworks.orchestration.hitl.intervention import (
    answer_intervention,
    emit_pending_questions,
    parse_question_response,
)
from choirworks.orchestration.planning.derived import spawn_followup_node
from choirworks.orchestration.planning.planner import plan_and_launch
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    NodeStatus,
    active_nodes,
    blocked_nodes,
    enqueue,
    has_pending_work,
    pending_interventions,
)
from choirworks.orchestration.transitions import apply_transition

logger = structlog.get_logger(__name__)


async def _prepare_inbound(
    ctx: OrchestrationContext, _payload: None
) -> Literal["recover", "ready"]:
    request = ctx.request
    assert request is not None
    if is_recover_request(request):
        logger.info("execute recover", task_id=ctx.task_id, context_id=ctx.context_id)
        return "recover"
    ctx.text = (request.get_user_input() or "").strip()
    if request.message is not None:
        try:
            ctx.responses = parse_question_response(request.message)
        except ValueError as exc:
            ctx.malformed = str(exc)
    if request.current_task is None:
        initial_task = new_task(
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            state=TaskState.TASK_STATE_SUBMITTED,
            history=[request.message] if request.message else None,
        )
        await ctx.queue.enqueue_event(initial_task)
    ctx.updater = TaskUpdater(ctx.queue, ctx.task_id, ctx.context_id)
    room = room_options(request.message)
    mentions = list(room.get("mentions") or [])
    for name in re.findall(r"@([A-Za-z0-9_-]+)", ctx.text):
        if name not in mentions:
            mentions.append(name)
    if mentions:
        room["mentions"] = mentions
    ctx.room = room
    return "ready"


async def _do_recover(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    await recover_session(ctx)
    return FlowOutcome.END


async def _do_reject(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    logger.info(
        "execute route=intervention_malformed", task_id=ctx.task_id, context_id=ctx.context_id
    )
    await emit_intervention_rejected(ctx, "", ctx.malformed or "")
    return FlowOutcome.END


async def _do_answers(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    logger.info(
        "execute route=intervention_answers",
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        answers=len(ctx.responses),
    )
    resolved = False
    for response in ctx.responses:
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


async def _do_reemit_questions(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    logger.info(
        "execute route=intervention_unanswered",
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        text=ctx.text,
    )
    await emit_pending_questions(ctx)
    return FlowOutcome.END


async def _do_complete(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    logger.info("execute route=empty_complete", task_id=ctx.task_id, context_id=ctx.context_id)
    updater = ctx.updater
    assert updater is not None
    await updater.complete()
    ctx.sessions.evict_session(ctx.context_id)
    return FlowOutcome.END


async def _do_plan_and_launch(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    logger.info(
        "execute route=plan_and_launch",
        task_id=ctx.task_id,
        context_id=ctx.context_id,
        text=ctx.text,
    )
    updater = ctx.updater
    assert updater is not None
    await updater.start_work()
    await plan_and_launch(ctx, ctx.text, room=ctx.room)
    return FlowOutcome.END


def _finish(ctx: OrchestrationContext) -> FlowOutcome:
    if ctx.needs_runner:
        start_runner(ctx)
    return FlowOutcome.END


async def _do_noop(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    return _finish(ctx)


async def _do_enqueue(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    node = ctx.target
    if node is None:
        return _finish(ctx)
    logger.info(
        "execute route=enqueue", task_id=ctx.task_id, context_id=ctx.context_id, node_id=node.id
    )
    _ = enqueue(ctx.state, node.id, ctx.text, sender="user", quote_id=ctx.quote_id)
    await ctx.sessions.persist(ctx)
    return _finish(ctx)


async def _do_spawn_followup(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    node = ctx.target
    if node is None:
        return _finish(ctx)
    logger.info(
        "execute route=followup", task_id=ctx.task_id, context_id=ctx.context_id, node_id=node.id
    )
    await spawn_followup_node(ctx, ctx.text, node)
    return _finish(ctx)


async def _do_interrupt(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    node = ctx.target
    if node is None:
        return _finish(ctx)
    logger.info(
        "execute route=interrupt", task_id=ctx.task_id, context_id=ctx.context_id, node_id=node.id
    )
    if node.a2a_task_id:
        try:
            agent_url = await agent_url_for(ctx, node.agent_name)
        except UnknownAgentError:
            logger.warning("interrupt: unknown agent", node=node.id, agent=node.agent_name)
        else:
            await ctx.remote.cancel_task(agent_url, node.a2a_task_id)
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
    await spawn_followup_node(ctx, ctx.text, node, deps=[])
    return _finish(ctx)


async def _do_new_plan(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    logger.info("execute route=new_plan", task_id=ctx.task_id, context_id=ctx.context_id)
    await plan_and_launch(ctx, ctx.text)
    return _finish(ctx)


async def _classify_inbound(
    ctx: OrchestrationContext, _payload: None
) -> Literal[
    "malformed",
    "answers",
    "unanswered_pending",
    "room",
    "empty",
    "plan",
]:
    if ctx.malformed is not None:
        return "malformed"
    if ctx.responses:
        return "answers"
    if pending_interventions(ctx.state):
        return "unanswered_pending"
    runner = ctx.runtime.runner
    if runner is not None and not runner.done():
        return "room"
    if has_pending_work(ctx.state):
        ctx.needs_runner = True
        return "room"
    if not ctx.text:
        return "empty"
    return "plan"


_QUOTED = Literal["quote_active", "quote_completed", "interrupt", "target", "new_plan", "noop"]


async def _classify_room(ctx: OrchestrationContext, _payload: None) -> _QUOTED:
    if not ctx.text:
        return "noop"
    state = ctx.state
    active = active_nodes(state)
    quote_id = ctx.room.get("quote_id")
    interrupt = bool(ctx.room.get("interrupt"))
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
            ctx.quote_id = str(quote_id)
            ctx.target = quoted
            return "quote_active"
        if quoted is not None and quoted.status == NodeStatus.COMPLETED:
            ctx.target = quoted
            return "quote_completed"
    if interrupt and active:
        ctx.target = active[0]
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
        ctx.quote_id = str(quote_id) if quote_id else None
        ctx.target = target
        return "target"
    return "new_plan"


message_flow: Flow[None] = Flow(
    name="message_flow",
    start=_prepare_inbound,
    edges=(
        Edge(_prepare_inbound, _do_recover, frozenset({"recover"})),
        Edge(_prepare_inbound, _classify_inbound, frozenset({"ready"})),
        Edge(_classify_inbound, _do_reject, frozenset({"malformed"})),
        Edge(_classify_inbound, _do_answers, frozenset({"answers"})),
        Edge(_classify_inbound, _do_reemit_questions, frozenset({"unanswered_pending"})),
        Edge(_classify_inbound, _classify_room, frozenset({"room"})),
        Edge(_classify_inbound, _do_complete, frozenset({"empty"})),
        Edge(_classify_inbound, _do_plan_and_launch, frozenset({"plan"})),
        Edge(_classify_room, _do_noop, frozenset({"noop"})),
        Edge(_classify_room, _do_enqueue, frozenset({"quote_active", "target"})),
        Edge(_classify_room, _do_spawn_followup, frozenset({"quote_completed"})),
        Edge(_classify_room, _do_interrupt, frozenset({"interrupt"})),
        Edge(_classify_room, _do_new_plan, frozenset({"new_plan"})),
    ),
)
