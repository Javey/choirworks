from __future__ import annotations

import asyncio

import structlog
from a2a.types.a2a_pb2 import TaskState

from choirworks.orchestration import intervention, repair
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import (
    emit_event,
    emit_function_call,
    emit_state_delta,
)
from choirworks.orchestration.node_executor import execute_node
from choirworks.orchestration.state import (
    NodeState,
    all_completed,
    failed_nodes,
    has_failures,
    has_pending_work,
    input_required_nodes,
    ready_nodes,
)

logger = structlog.get_logger(__name__)


def start_runner(ctx: OrchestrationContext) -> None:
    runtime = ctx.runtime
    if runtime.runner is not None and not runtime.runner.done():
        return
    runtime.runner = asyncio.create_task(run_plan(ctx), name=f"choirworks-runner:{ctx.context_id}")
    logger.info("start_runner", context_id=ctx.context_id)


async def run_plan(ctx: OrchestrationContext) -> None:
    """Rule-based scheduler loop (not LLM-backed).

    Dispatches ready nodes in parallel waves, waits for completion,
    retries failed nodes, settles input, repairs/replans, and emits
    terminal events.  Delegates to subagents for LLM decisions.
    """
    runtime = ctx.runtime
    context_id = ctx.context_id
    task_id = ctx.task_id
    try:
        while True:
            async with ctx.lock:
                state = ctx.state
                for node in list(failed_nodes(state)):
                    if node.attempt < ctx.config.max_node_attempts:
                        node.status = "pending"
                        node.error = None

                ready = ready_nodes(state)
                slots = max(0, ctx.config.max_parallel - _pending_count(ctx))
                batch: list[tuple[NodeState, str]] = []
                for node in ready[:slots]:
                    mode = (
                        "recover"
                        if node.status == "recover"
                        else ("continue" if node.status == "ready" else "dispatch")
                    )
                    if mode != "recover":
                        node.status = "dispatched"
                    batch.append((node, mode))
                if batch:
                    await _announce_dispatch(ctx, batch)
                    logger.info(
                        "run_plan dispatched",
                        task_id=task_id,
                        context_id=context_id,
                        dispatched=[(n.id, n.agent_name, m) for n, m in batch],
                    )
                for node, mode in batch:
                    node_task = asyncio.create_task(
                        execute_node(ctx, node, mode=mode),
                        name=f"choirworks-node:{context_id}:{node.id}",
                    )
                    runtime.node_tasks[node_task] = node
                if batch:
                    await ctx.sessions.persist(ctx)

            pending = dict(runtime.node_tasks)
            if pending:
                done, _ = await asyncio.wait(set(pending), return_when=asyncio.FIRST_COMPLETED)
                for finished in done:
                    node = runtime.node_tasks.pop(finished)
                    exception = finished.exception()
                    if exception is not None:
                        node.status = "failed"
                        node.error = str(exception)
                        logger.warning(
                            "run_plan raised",
                            task_id=task_id,
                            context_id=context_id,
                            node_id=node.id,
                            error=exception,
                        )
                        await emit_state_delta(
                            ctx,
                            nodes={
                                node.id: {"status": "failed", "error": node.error},
                            },
                        )
                if ctx.config.retry_backoff > 0 and any(
                    n.status == "failed" and n.attempt < ctx.config.max_node_attempts
                    for n in ctx.state.nodes.values()
                ):
                    await asyncio.sleep(ctx.config.retry_backoff)
                continue

            async with ctx.lock:
                state = ctx.state
                if any(
                    n.status == "failed" and n.attempt < ctx.config.max_node_attempts
                    for n in state.nodes.values()
                ):
                    continue
                if input_required_nodes(state):
                    progress = await intervention.settle_input(ctx)
                    if progress:
                        continue
                    await ctx.sessions.persist(ctx)
                    logger.info(
                        "run_plan state=input_required", task_id=task_id, context_id=context_id
                    )
                    await emit_event(
                        ctx,
                        "",
                        TaskState.TASK_STATE_INPUT_REQUIRED,
                    )
                    return
                if all_completed(state):
                    logger.info("run_plan state=completed", task_id=task_id, context_id=context_id)
                    await emit_event(
                        ctx,
                        "",
                        TaskState.TASK_STATE_COMPLETED,
                    )
                    ctx.sessions.evict_session(context_id)
                    return
                if has_failures(state):
                    recovered = False
                    if (
                        ctx.config.replan_on_failure
                        and state.revision_count < ctx.config.max_revisions
                    ):
                        logger.info(
                            "run_plan attempting repair",
                            task_id=task_id,
                            context_id=context_id,
                            revision=state.revision_count + 1,
                            max_revisions=ctx.config.max_revisions,
                        )
                        recovered = await repair.repair_plan(ctx)
                    if recovered:
                        logger.info(
                            "run_plan repair succeeded", task_id=task_id, context_id=context_id
                        )
                        continue
                    logger.info("run_plan state=failed", task_id=task_id, context_id=context_id)
                    await emit_event(
                        ctx,
                        "",
                        TaskState.TASK_STATE_FAILED,
                    )
                    ctx.sessions.evict_session(context_id)
                    return
                if has_pending_work(state):
                    schedulable = bool(ready_nodes(state)) or (_pending_count(ctx) > 0)
                    if schedulable:
                        await asyncio.sleep(0)
                        continue
                    logger.warning(
                        "Runner stalled for task",
                        task_id=task_id,
                        context_id=context_id,
                        nodes={node.id: node.status for node in state.nodes.values()},
                    )
                else:
                    logger.warning(
                        "Runner stalled for task", task_id=task_id, context_id=context_id
                    )
                await emit_event(
                    ctx,
                    "",
                    TaskState.TASK_STATE_FAILED,
                )
                ctx.sessions.evict_session(context_id)
                return
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Runner failed for task", task_id=task_id, context_id=context_id)
        if context_id in ctx.sessions.sessions:
            try:
                await ctx.sessions.persist(ctx)
                await emit_event(
                    ctx,
                    "",
                    TaskState.TASK_STATE_FAILED,
                )
            except Exception:
                logger.exception(
                    "Failed to emit task failure for", task_id=task_id, context_id=context_id
                )
            ctx.sessions.evict_session(context_id)
    finally:
        runtime.runner = None
        for node_task in list(runtime.node_tasks):
            node_task.cancel()
        runtime.node_tasks.clear()


def _pending_count(ctx: OrchestrationContext) -> int:
    return len(ctx.runtime.node_tasks)


async def _announce_dispatch(
    ctx: OrchestrationContext,
    batch: list[tuple[NodeState, str]],
) -> None:
    from choirworks.tools import CallSubagentArgs, FunctionResult, call_subagent_func

    for node, mode in batch:
        if mode != "dispatch" or node.derived or node.attempt != 0:
            continue
        args = CallSubagentArgs(
            requested_by="orchestrator",
            target_agent=node.agent_name,
            instruction=node.input_text or node.name,
        )
        await emit_function_call(ctx, call_subagent_func, args, FunctionResult(success=True))
