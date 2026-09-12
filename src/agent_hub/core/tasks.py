from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from agent_hub.a2a.registry import AgentRegistry
from agent_hub.models.domain import Node, OrchestrationTask, Plan
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


class TaskNotFound(KeyError):
    pass


class UnknownAgent(ValueError):
    pass


@dataclass
class TargetSpec:
    agent_name: str
    skill_id: str | None = None
    name: str = "single"
    input: dict[str, Any] | None = None


class CreatedTask(BaseModel):
    task_id: str
    plan_id: str
    node_ids: list[str]


class TaskSnapshot(BaseModel):
    task: OrchestrationTask
    plan: Plan | None
    nodes: list[Node]


class TaskService:
    def __init__(self, db: Database, event_store: EventStore, registry: AgentRegistry):
        self._db = db
        self._events = event_store
        self._registry = registry

    async def create_task(self, request: str, target: TargetSpec) -> CreatedTask:
        record = await self._registry.get_by_name(target.agent_name)
        if record is None:
            raise UnknownAgent(f"agent not registered: {target.agent_name}")
        agent_url = self._registry.agent_url(record)

        task_id = uuid4().hex
        plan_id = uuid4().hex
        dag_node_id = "n1"
        node_id = f"{plan_id}:{dag_node_id}"
        node_input = target.input or {"text": request}
        dag = {
            "nodes": [
                {
                    "id": dag_node_id,
                    "name": target.name,
                    "agent_url": agent_url,
                    "skill_id": target.skill_id,
                    "deps": [],
                    "input": node_input,
                    "requires_approval": False,
                    "policy_override": None,
                }
            ]
        }

        await self._events.append(
            task_id, EventType.TASK_CREATED, {"request": request, "policy": None}
        )
        await self._events.append(
            task_id,
            EventType.PLAN_CREATED,
            {
                "plan_id": plan_id,
                "version": 1,
                "rationale": "manual single-node target",
                "dag": dag,
            },
        )
        await self._events.append(
            task_id,
            EventType.TASK_STATE_CHANGED,
            {"from": TaskStatus.PLANNING.value, "to": TaskStatus.RUNNING.value},
        )
        return CreatedTask(task_id=task_id, plan_id=plan_id, node_ids=[node_id])

    async def get_snapshot(self, task_id: str) -> TaskSnapshot:
        task = await projections.fetch_task(self._db, task_id)
        if task is None:
            raise TaskNotFound(task_id)
        plan = await projections.fetch_current_plan(self._db, task_id)
        nodes = await projections.fetch_nodes(
            self._db, task_id, plan.id if plan else None
        )
        return TaskSnapshot(task=task, plan=plan, nodes=nodes)

    async def finalize_if_complete(self, task_id: str) -> OrchestrationTask:
        task = await projections.fetch_task(self._db, task_id)
        if task is None:
            raise TaskNotFound(task_id)
        plan = await projections.fetch_current_plan(self._db, task_id)
        if task.status is not TaskStatus.RUNNING or plan is None:
            return task
        nodes = await projections.fetch_nodes(self._db, task_id, plan.id)
        if not nodes:
            return task
        statuses = {node.status for node in nodes}
        active = {
            NodeStatus.PENDING,
            NodeStatus.READY,
            NodeStatus.DISPATCHED,
            NodeStatus.WORKING,
            NodeStatus.INPUT_REQUIRED,
        }
        if all(status is NodeStatus.COMPLETED for status in statuses):
            await self._events.append(task_id, EventType.TASK_COMPLETED, {})
        elif not statuses & active:
            await self._events.append(task_id, EventType.TASK_FAILED, {})
        refreshed = await projections.fetch_task(self._db, task_id)
        assert refreshed is not None
        return refreshed
