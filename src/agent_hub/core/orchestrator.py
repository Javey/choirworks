from __future__ import annotations

import asyncio
import json

from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.planner import Planner
from agent_hub.core.tasks import TaskService
from agent_hub.models.domain import Node, OrchestrationTask
from agent_hub.models.enums import (
    TERMINAL_TASK_STATUSES,
    EventType,
    NodeStatus,
    TaskStatus,
)
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


class Orchestrator:
    def __init__(
        self,
        db: Database,
        events: EventStore,
        planner: Planner,
        dispatcher: NodeDispatcher,
        task_service: TaskService,
        *,
        max_parallel: int = 5,
        max_node_attempts: int = 2,
        retry_backoff_seconds: float = 1.0,
        replan_on_failure: bool = True,
    ):
        self._db = db
        self._events = events
        self._planner = planner
        self._dispatcher = dispatcher
        self._task_service = task_service
        self._max_parallel = max_parallel
        self._max_node_attempts = max_node_attempts
        self._retry_backoff = retry_backoff_seconds
        self._replan_on_failure = replan_on_failure
        self._runs: dict[str, asyncio.Task] = {}
        self._inflight: set[asyncio.Task] = set()

    def start(self, task_id: str) -> None:
        if task_id in self._runs and not self._runs[task_id].done():
            return
        run = asyncio.create_task(self.run(task_id), name=f"orchestrator:{task_id}")
        self._runs[task_id] = run
        run.add_done_callback(lambda _: self._runs.pop(task_id, None))

    async def stop(self) -> None:
        pending = list(self._runs.values()) + list(self._inflight)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._runs.clear()
        self._inflight.clear()

    async def wait(self, task_id: str, timeout_seconds: float = 30.0) -> None:
        async def _poll() -> None:
            while True:
                task = await projections.fetch_task(self._db, task_id)
                if task is not None and (
                    task.status in TERMINAL_TASK_STATUSES
                    or task.status is TaskStatus.AWAITING_INPUT
                ):
                    return
                await asyncio.sleep(0.02)

        async with asyncio.timeout(timeout_seconds):
            await _poll()

    async def run(self, task_id: str) -> None:
        task = await projections.fetch_task(self._db, task_id)
        if task is None or task.status in TERMINAL_TASK_STATUSES:
            return
        if task.plan_version is None:
            await self._initial_plan(task)

        while True:
            task = await projections.fetch_task(self._db, task_id)
            if task is None or task.status in TERMINAL_TASK_STATUSES:
                return
            plan = await projections.fetch_current_plan(self._db, task_id)
            if plan is None:
                return
            nodes = await projections.fetch_nodes(self._db, task_id, plan.id)
            self._inflight = {item for item in self._inflight if not item.done()}

            failed = [node for node in nodes if node.status is NodeStatus.FAILED]
            if failed:
                await self._handle_failure(task, failed[0])
                continue

            parked = [node for node in nodes if node.status is NodeStatus.INPUT_REQUIRED]
            if parked:
                if task.status is not TaskStatus.AWAITING_INPUT:
                    await self._events.append(
                        task_id,
                        EventType.TASK_STATE_CHANGED,
                        {
                            "from": TaskStatus.RUNNING.value,
                            "to": TaskStatus.AWAITING_INPUT.value,
                        },
                    )
                return

            ready = [
                node
                for node in nodes
                if node.status is NodeStatus.PENDING and self._deps_completed(node, nodes)
            ] + [node for node in nodes if node.status is NodeStatus.READY]

            if ready:
                slots = max(0, self._max_parallel - len(self._inflight))
                for node in ready[:slots]:
                    self._inflight.add(
                        asyncio.create_task(
                            self._dispatcher.dispatch_node(task_id, node.id)
                        )
                    )
                if self._inflight:
                    await asyncio.wait(
                        self._inflight, return_when=asyncio.FIRST_COMPLETED
                    )
                continue

            if self._inflight:
                await asyncio.wait(self._inflight, return_when=asyncio.FIRST_COMPLETED)
                continue

            if nodes and all(node.status is NodeStatus.COMPLETED for node in nodes):
                await self._task_service.finalize_if_complete(task_id)
                return

            await self._events.append(
                task_id,
                EventType.ERROR,
                {"message": "scheduler stalled: no ready nodes and no in-flight work"},
            )
            await self._events.append(task_id, EventType.TASK_FAILED, {})
            return

    async def _initial_plan(self, task: OrchestrationTask) -> None:
        try:
            drafted = await self._planner.plan(task.request)
            await self._task_service.create_plan_from_draft(task.id, drafted, version=1)
            await self._task_service.mark_running(task.id)
        except Exception as exc:  # noqa: BLE001 - 规划失败统一标记任务失败
            await self._events.append(task.id, EventType.ERROR, {"message": str(exc)})
            await self._events.append(task.id, EventType.TASK_FAILED, {})

    async def _handle_failure(self, task: OrchestrationTask, node: Node) -> None:
        if node.attempt < self._max_node_attempts:
            delay = self._retry_backoff * node.attempt
            await self._events.append(
                task.id,
                EventType.NODE_RETRY_SCHEDULED,
                {
                    "node_id": node.id,
                    "attempt": node.attempt + 1,
                    "delay_seconds": delay,
                },
            )
            if delay > 0:
                await asyncio.sleep(delay)
            await self._events.append(
                task.id,
                EventType.NODE_STATE_CHANGED,
                {
                    "node_id": node.id,
                    "from": node.status.value,
                    "to": NodeStatus.READY.value,
                },
            )
            return
        if self._replan_on_failure:
            await self._replan(task, node)
            return
        await self._events.append(task.id, EventType.TASK_FAILED, {})

    async def _replan(self, task: OrchestrationTask, failed_node: Node) -> None:
        plan = await projections.fetch_current_plan(self._db, task.id)
        assert plan is not None
        nodes = await projections.fetch_nodes(self._db, task.id, plan.id)
        completed = [node for node in nodes if node.status is NodeStatus.COMPLETED]
        context = "\n".join(
            f"- {node.name}: {json.dumps(node.output, ensure_ascii=False)}"
            for node in completed
        )
        reason = f"node '{failed_node.name}' failed: {failed_node.error or 'unknown error'}"
        try:
            drafted = await self._planner.plan(
                task.request, reason=reason, context=context
            )
            await self._task_service.create_plan_from_draft(
                task.id, drafted, version=plan.version + 1
            )
        except Exception as exc:  # noqa: BLE001 - 重规划失败统一标记任务失败
            await self._events.append(task.id, EventType.ERROR, {"message": str(exc)})
            await self._events.append(task.id, EventType.TASK_FAILED, {})

    @staticmethod
    def _deps_completed(node: Node, nodes: list[Node]) -> bool:
        by_id = {item.id: item for item in nodes}
        return all(
            dep in by_id and by_id[dep].status is NodeStatus.COMPLETED
            for dep in node.deps
        )
