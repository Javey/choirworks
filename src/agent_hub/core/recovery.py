from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.models.enums import TERMINAL_TASK_STATUSES, NodeStatus
from agent_hub.store import projections


@dataclass
class RecoveryResult:
    recovered_task_ids: list[str] = field(default_factory=list)
    background: list[asyncio.Task] = field(default_factory=list)


async def recover_tasks(
    db,
    events,
    remote,
    dispatcher: NodeDispatcher,
    orchestrator: Orchestrator,
) -> RecoveryResult:
    """扫描非终态任务并恢复调度。"""
    terminal = {status.value for status in TERMINAL_TASK_STATUSES}
    cursor = await db.conn.execute("SELECT id, status FROM orchestration_tasks")
    result = RecoveryResult()
    for row in await cursor.fetchall():
        if row["status"] in terminal:
            continue
        task_id = row["id"]
        plan = await projections.fetch_current_plan(db, task_id)
        if plan is None:
            orchestrator.start(task_id)
            result.recovered_task_ids.append(task_id)
            continue
        nodes = await projections.fetch_nodes(db, task_id, plan.id)
        inflight = [
            node
            for node in nodes
            if node.status in (NodeStatus.DISPATCHED, NodeStatus.WORKING)
            and node.a2a_task_id
        ]
        if inflight:

            async def _resume_and_run(
                tid: str = task_id, ids: list[str] | None = None
            ) -> None:
                node_ids = ids or []
                await asyncio.gather(
                    *(dispatcher.resume_node(tid, node_id) for node_id in node_ids),
                    return_exceptions=True,
                )
                orchestrator.start(tid)

            task = asyncio.create_task(
                _resume_and_run(task_id, [node.id for node in inflight])
            )
            result.background.append(task)
        else:
            orchestrator.start(task_id)
        result.recovered_task_ids.append(task_id)
    return result
