# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_graph.py、utils/_graph_validation.py
from __future__ import annotations

import asyncio
from typing import Literal

import structlog
from a2a.types.a2a_pb2 import TaskState

from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_event, emit_pending_questions
from choirworks.orchestration.graph import DEFAULT, Edge, Flow, FlowOutcome
from choirworks.orchestration.planning import repair
from choirworks.orchestration.state import (
    all_completed,
    has_failures,
    has_pending_work,
    input_required_nodes,
    pending_interventions,
    ready_nodes,
)
from choirworks.orchestration.transitions import can_retry

logger = structlog.get_logger(__name__)


async def _check_retryable(ctx: OrchestrationContext, _payload: None) -> Literal["retry", "next"]:
    if any(can_retry(n, ctx.config.max_node_attempts) for n in ctx.state.nodes.values()):
        return "retry"
    return "next"


async def _backoff_continue(_ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    return FlowOutcome.CONTINUE


async def _check_input_required(
    ctx: OrchestrationContext, _payload: None
) -> Literal["wait_input", "next"]:
    if input_required_nodes(ctx.state):
        return "wait_input"
    return "next"


async def _exit_wait(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    state = ctx.state
    await ctx.sessions.persist(ctx)
    logger.info("run_plan state=input_required", task_id=ctx.task_id, context_id=ctx.context_id)
    if pending_interventions(state):
        await emit_pending_questions(ctx)
    else:
        await emit_event(ctx, "", TaskState.TASK_STATE_INPUT_REQUIRED)
    return FlowOutcome.EXIT_WAIT


async def _check_all_completed(
    ctx: OrchestrationContext, _payload: None
) -> Literal["plan_completed", "next"]:
    if all_completed(ctx.state):
        return "plan_completed"
    return "next"


async def _emit_completed(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    logger.info("run_plan state=completed", task_id=ctx.task_id, context_id=ctx.context_id)
    await emit_event(ctx, "", TaskState.TASK_STATE_COMPLETED)
    ctx.sessions.evict_session(ctx.context_id)
    return FlowOutcome.EXIT_DONE


async def _check_repairable(ctx: OrchestrationContext, _payload: None) -> Literal["repair", "next"]:
    if has_failures(ctx.state):
        return "repair"
    return "next"


async def _repair(ctx: OrchestrationContext, _payload: None) -> Literal["repair_ok", "__default__"]:
    state = ctx.state
    if ctx.config.replan_on_failure and state.revision_count < ctx.config.max_revisions:
        logger.info(
            "run_plan attempting repair",
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            revision=state.revision_count + 1,
            max_revisions=ctx.config.max_revisions,
        )
        if await repair.repair_plan(ctx):
            logger.info("run_plan repair succeeded", task_id=ctx.task_id, context_id=ctx.context_id)
            return "repair_ok"
    return DEFAULT


async def _emit_failed(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    logger.info("run_plan state=failed", task_id=ctx.task_id, context_id=ctx.context_id)
    await emit_event(ctx, "", TaskState.TASK_STATE_FAILED)
    ctx.sessions.evict_session(ctx.context_id)
    return FlowOutcome.EXIT_FAILED


async def _stalled(ctx: OrchestrationContext, _payload: None) -> FlowOutcome:
    state = ctx.state
    if has_pending_work(state) and (ready_nodes(state) or len(ctx.runtime.node_tasks) > 0):
        await asyncio.sleep(0)
        return FlowOutcome.CONTINUE
    if has_pending_work(state):
        logger.warning(
            "Runner stalled for task",
            task_id=ctx.task_id,
            context_id=ctx.context_id,
            nodes={node.id: node.status for node in state.nodes.values()},
        )
    else:
        logger.warning("Runner stalled for task", task_id=ctx.task_id, context_id=ctx.context_id)
    await emit_event(ctx, "", TaskState.TASK_STATE_FAILED)
    ctx.sessions.evict_session(ctx.context_id)
    return FlowOutcome.EXIT_FAILED


plan_flow: Flow[None] = Flow(
    name="plan_flow",
    start="check_retryable",
    handlers={
        "check_retryable": _check_retryable,
        "backoff_continue": _backoff_continue,
        "check_input_required": _check_input_required,
        "exit_wait": _exit_wait,
        "check_all_completed": _check_all_completed,
        "emit_completed": _emit_completed,
        "check_repairable": _check_repairable,
        "repair": _repair,
        "emit_failed": _emit_failed,
        "stalled": _stalled,
    },
    edges=(
        Edge("check_retryable", "backoff_continue", frozenset({"retry"})),
        Edge("check_retryable", "check_input_required", frozenset({"next"})),
        Edge("check_input_required", "exit_wait", frozenset({"wait_input"})),
        Edge("check_input_required", "check_all_completed", frozenset({"next"})),
        Edge("check_all_completed", "emit_completed", frozenset({"plan_completed"})),
        Edge("check_all_completed", "check_repairable", frozenset({"next"})),
        Edge("check_repairable", "repair", frozenset({"repair"})),
        Edge("check_repairable", "stalled", frozenset({"next"})),
        Edge("repair", "backoff_continue", frozenset({"repair_ok"})),
        Edge("repair", "emit_failed", frozenset({DEFAULT})),
    ),
)
plan_flow.validate()
