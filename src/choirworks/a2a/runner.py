from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from a2a.types.a2a_pb2 import TaskState

from choirworks.a2a.context import OrchestrationContext
from choirworks.a2a.state import NodeState

if TYPE_CHECKING:
    from choirworks.a2a.events import EventEmitter
    from choirworks.a2a.intervention import InterventionManager
    from choirworks.a2a.node_executor import NodeExecutor
    from choirworks.a2a.repair import RepairManager
    from choirworks.a2a.session import SessionManager

logger = logging.getLogger(__name__)


class PlanRunner:
    """Rule-based scheduler loop (not LLM-backed).

    Dispatches ready nodes in parallel waves, waits for completion,
    retries failed nodes, settles input, repairs/replans, and emits
    terminal events.  Delegates to subagents for LLM decisions.
    """

    def __init__(
        self,
        emitter: EventEmitter,
        session_mgr: SessionManager,
        node_executor: NodeExecutor,
        intervention_mgr: InterventionManager,
        repair_mgr: RepairManager,
        config: object,
    ):
        self._emitter = emitter
        self._session_mgr = session_mgr
        self._node_executor = node_executor
        self._intervention_mgr = intervention_mgr
        self._repair_mgr = repair_mgr
        self._config = config

    def start_runner(self, ctx: OrchestrationContext) -> None:
        runtime = ctx.runtime
        if runtime.runner is not None and not runtime.runner.done():
            return
        runtime.runner = asyncio.create_task(
            self.run_plan(ctx), name=f"choirworks-runner:{ctx.context_id}"
        )

    async def run_plan(self, ctx: OrchestrationContext) -> None:
        runtime = ctx.runtime
        context_id = ctx.context_id
        task_id = ctx.task_id
        try:
            while True:
                async with ctx.lock:
                    state = ctx.state
                    for node in list(state.failed_nodes()):
                        if node.attempt < self._config.max_node_attempts:
                            node.status = "pending"
                            node.error = None

                    ready = state.ready_nodes()
                    slots = max(0, self._config.max_parallel - self._pending_count(ctx))
                    batch: list[tuple[NodeState, str]] = []
                    for node in ready[:slots]:
                        mode = (
                            "resume"
                            if node.status == "resume"
                            else ("continue" if node.status == "ready" else "dispatch")
                        )
                        if mode != "resume":
                            node.status = "dispatched"
                        batch.append((node, mode))
                    if batch:
                        await self._announce_dispatch(ctx, batch)
                    for node, mode in batch:
                        node_task = asyncio.create_task(
                            self._node_executor.execute_node(ctx, node, mode=mode),
                            name=f"choirworks-node:{context_id}:{node.id}",
                        )
                        runtime.node_tasks[node_task] = node
                    if batch:
                        await self._session_mgr.persist(ctx)

                pending = dict(runtime.node_tasks)
                if pending:
                    done, _ = await asyncio.wait(
                        set(pending), return_when=asyncio.FIRST_COMPLETED
                    )
                    for finished in done:
                        node = runtime.node_tasks.pop(finished)
                        exception = finished.exception()
                        if exception is not None:
                            node.status = "failed"
                            node.error = str(exception)
                            await self._emitter.emit_state_delta(ctx, nodes={
                                node.id: {"status": "failed", "error": node.error},
                            })
                    if self._config.retry_backoff > 0 and any(
                        n.status == "failed" and n.attempt < self._config.max_node_attempts
                        for n in ctx.state.nodes.values()
                    ):
                        await asyncio.sleep(self._config.retry_backoff)
                    continue

                async with ctx.lock:
                    state = ctx.state
                    if any(
                        n.status == "failed" and n.attempt < self._config.max_node_attempts
                        for n in state.nodes.values()
                    ):
                        continue
                    if state.input_required_nodes():
                        progress = await self._intervention_mgr.settle_input(ctx)
                        if progress:
                            continue
                        await self._session_mgr.persist(ctx)
                        await self._emitter.emit_event(
                            ctx, "", TaskState.TASK_STATE_INPUT_REQUIRED,
                        )
                        return
                    if state.all_completed():
                        await self._emitter.emit_event(
                            ctx, "", TaskState.TASK_STATE_COMPLETED,
                        )
                        self._session_mgr.evict_session(context_id)
                        return
                    if state.has_failures():
                        recovered = False
                        if (
                            self._config.replan_on_failure
                            and state.revision_count < self._config.max_revisions
                        ):
                            recovered = await self._repair_mgr.repair_plan(ctx)
                        if recovered:
                            continue
                        await self._emitter.emit_event(
                            ctx, "", TaskState.TASK_STATE_FAILED,
                        )
                        self._session_mgr.evict_session(context_id)
                        return
                    if state.has_pending_work():
                        schedulable = bool(state.ready_nodes()) or (
                            self._pending_count(ctx) > 0
                        )
                        if schedulable:
                            await asyncio.sleep(0)
                            continue
                        logger.warning(
                            "Runner stalled for task %s: %s",
                            task_id,
                            {node.id: node.status for node in state.nodes.values()},
                        )
                    else:
                        logger.warning("Runner stalled for task %s", task_id)
                    await self._emitter.emit_event(
                        ctx, "", TaskState.TASK_STATE_FAILED,
                    )
                    self._session_mgr.evict_session(context_id)
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Runner failed for task %s", task_id)
            if context_id in self._session_mgr.sessions:
                try:
                    await self._session_mgr.persist(ctx)
                    await self._emitter.emit_event(
                        ctx, "", TaskState.TASK_STATE_FAILED,
                    )
                except Exception:
                    logger.exception("Failed to emit task failure for %s", task_id)
                self._session_mgr.evict_session(context_id)
        finally:
            runtime.runner = None
            for node_task in list(runtime.node_tasks):
                node_task.cancel()
            runtime.node_tasks.clear()

    def _pending_count(self, ctx: OrchestrationContext) -> int:
        return len(ctx.runtime.node_tasks)

    async def _announce_dispatch(
        self,
        ctx: OrchestrationContext,
        batch: list[tuple[NodeState, str]],
    ) -> None:
        from choirworks.tools import CallSubagentArgs, FunctionResult
        func = ctx.runtime.call_subagent_func
        if func is None:
            return
        for node, mode in batch:
            if mode != "dispatch" or node.derived or node.attempt != 0:
                continue
            args = CallSubagentArgs(
                requested_by="orchestrator",
                target_agent=node.agent_name,
                instruction=node.name,
            )
            await self._emitter.emit_function_call(
                ctx, func, args, FunctionResult(success=True)
            )
