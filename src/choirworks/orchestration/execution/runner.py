from __future__ import annotations

import asyncio

import structlog
from a2a.types.a2a_pb2 import TaskState

from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_event, emit_function_call, emit_state_delta
from choirworks.orchestration.execution.node_executor import execute_node
from choirworks.orchestration.flows import settlement
from choirworks.orchestration.flows.engine import FlowOutcome
from choirworks.orchestration.flows.plan import plan_flow
from choirworks.orchestration.state import (
    ACTIVE_NODE_STATUSES,
    PENDING_NODE_STATUSES,
    NodeState,
    NodeStatus,
    OrchestrationState,
    failed_nodes,
    input_required_nodes,
    pending_intervention_for,
    ready_nodes,
)
from choirworks.orchestration.transitions import can_retry, can_transition, transition

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
            runtime.wake.clear()
            async with ctx.lock:
                state = ctx.state
                for node in list(failed_nodes(state)):
                    if can_retry(node, ctx.config.max_node_attempts):
                        await transition(ctx, node, NodeStatus.PENDING, emit=False)
                        node.error = None

                _spawn_settlements(ctx)

                ready = ready_nodes(state)
                slots = max(0, ctx.config.max_parallel - _pending_count(ctx))
                batch: list[tuple[NodeState, str]] = []
                for node in ready[:slots]:
                    if node.status == NodeStatus.RECOVER:
                        mode = "recover"
                    elif node.status == NodeStatus.READY:
                        mode = "continue"
                    else:
                        mode = "dispatch"
                    if mode != "recover":
                        await transition(ctx, node, NodeStatus.SUBMITTED, emit=False)
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

            waiters = set(runtime.node_tasks) | set(runtime.settle_tasks)
            if waiters:
                wake_task = asyncio.create_task(
                    _wait_for_wake(runtime.wake), name=f"choirworks-wake:{context_id}"
                )
                done, _ = await asyncio.wait(
                    [*waiters, wake_task], return_when=asyncio.FIRST_COMPLETED
                )
                if wake_task in done:
                    runtime.wake.clear()
                else:
                    wake_task.cancel()
                    try:
                        await wake_task
                    except asyncio.CancelledError:
                        pass
                for finished in done:
                    if finished is wake_task:
                        continue
                    if finished in runtime.node_tasks:
                        node = runtime.node_tasks.pop(finished)
                        exception = finished.exception()
                        if exception is not None:
                            if can_transition(node, NodeStatus.FAILED):
                                await transition(ctx, node, NodeStatus.FAILED, emit=False)
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
                    elif finished in runtime.settle_tasks:
                        node_id = runtime.settle_tasks.pop(finished)
                        exception = finished.exception()
                        if exception is not None:
                            logger.warning(
                                "run_plan settle raised",
                                task_id=task_id,
                                context_id=context_id,
                                node_id=node_id,
                                error=exception,
                            )
                if ctx.config.retry_backoff > 0 and any(
                    can_retry(n, ctx.config.max_node_attempts) for n in ctx.state.nodes.values()
                ):
                    await asyncio.sleep(ctx.config.retry_backoff)
                continue

            async with ctx.lock:
                outcome = await plan_flow.run(ctx, None)
            if outcome is FlowOutcome.CONTINUE:
                continue
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
        for settle_task in list(runtime.settle_tasks):
            settle_task.cancel()
        runtime.settle_tasks.clear()


def _spawn_settlements(ctx: OrchestrationContext) -> None:
    """Spawn per-node background settlement for undecided input_required nodes."""
    runtime = ctx.runtime
    state = ctx.state
    settled = set(runtime.settle_tasks.values())
    for node in input_required_nodes(state):
        if node.id in settled:
            continue
        if pending_intervention_for(state, node.id) is not None:
            continue
        if _has_active_helper(state, node.id):
            continue
        task = asyncio.create_task(
            _run_settlement(ctx, node),
            name=f"choirworks-settle:{ctx.context_id}:{node.id}",
        )
        runtime.settle_tasks[task] = node.id
        logger.info("run_plan settle spawned", task_id=ctx.task_id, node_id=node.id)


async def _wait_for_wake(event: asyncio.Event) -> None:
    await event.wait()


async def _run_settlement(ctx: OrchestrationContext, node: NodeState) -> None:
    await settlement.settle_node_input(ctx, node)


def _has_active_helper(state: OrchestrationState, node_id: str) -> bool:
    return any(
        n.derived
        and n.assist_requested_by == node_id
        and n.status in ACTIVE_NODE_STATUSES | PENDING_NODE_STATUSES
        for n in state.nodes.values()
    )


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
