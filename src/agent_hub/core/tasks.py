from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.planner import PlanDraft, PlanNodeDraft, draft_to_dag
from agent_hub.models.domain import Checkpoint, Node, OrchestrationTask, Plan
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


class TaskNotFound(KeyError):
    pass


class UnknownAgent(ValueError):
    pass


class ConversationNotFound(KeyError):
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
    conversation_id: str | None = None


class TaskSnapshot(BaseModel):
    task: OrchestrationTask
    plan: Plan | None
    nodes: list[Node]
    last_seq: int = 0


class TaskService:
    def __init__(self, db: Database, event_store: EventStore, registry: AgentRegistry):
        self._db = db
        self._events = event_store
        self._registry = registry

    async def _resolve_conversation(
        self, request: str, conversation_id: str | None
    ) -> tuple[str, str | None]:
        if conversation_id is not None:
            conversation = await projections.fetch_conversation(self._db, conversation_id)
            if conversation is None:
                raise ConversationNotFound(f"conversation not found: {conversation_id}")
            return conversation_id, None
        return uuid4().hex, request[:60]

    async def create_task(
        self, request: str, target: TargetSpec, conversation_id: str | None = None
    ) -> CreatedTask:
        record = await self._registry.get_by_name(target.agent_name)
        if record is None:
            raise UnknownAgent(f"agent not registered: {target.agent_name}")
        agent_url = self._registry.agent_url(record)
        resolved_conversation_id, conversation_title = await self._resolve_conversation(
            request, conversation_id
        )

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
            task_id,
            EventType.TASK_CREATED,
            {
                "request": request,
                "policy": None,
                "conversation_id": resolved_conversation_id,
                "conversation_title": conversation_title,
            },
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
        return CreatedTask(
            task_id=task_id,
            plan_id=plan_id,
            node_ids=[node_id],
            conversation_id=resolved_conversation_id,
        )

    async def create_pending_task(
        self, request: str, conversation_id: str | None = None
    ) -> str:
        resolved_conversation_id, conversation_title = await self._resolve_conversation(
            request, conversation_id
        )
        task_id = uuid4().hex
        await self._events.append(
            task_id,
            EventType.TASK_CREATED,
            {
                "request": request,
                "policy": None,
                "conversation_id": resolved_conversation_id,
                "conversation_title": conversation_title,
            },
        )
        return task_id

    async def create_plan_from_draft(
        self, task_id: str, draft: PlanDraft, *, version: int
    ) -> CreatedTask:
        records = await self._registry.list()
        agent_urls = {
            record.name: self._registry.agent_url(record) for record in records
        }
        plan_id = uuid4().hex
        dag = draft_to_dag(draft, agent_urls)
        if version > 1:
            previous = await projections.fetch_current_plan(self._db, task_id)
            await self._events.append(
                task_id,
                EventType.PLAN_SUPERSEDED,
                {
                    "plan_id": previous.id if previous else None,
                    "superseded_by_version": version,
                },
            )
        await self._events.append(
            task_id,
            EventType.PLAN_CREATED,
            {
                "plan_id": plan_id,
                "version": version,
                "rationale": draft.rationale,
                "dag": dag,
            },
        )
        return CreatedTask(
            task_id=task_id,
            plan_id=plan_id,
            node_ids=[f"{plan_id}:{node.id}" for node in draft.nodes],
        )

    async def extend_plan(
        self,
        task_id: str,
        *,
        added_nodes: list[PlanNodeDraft],
        edges: list[tuple[str, str]],
        rationale: str,
    ) -> Plan:
        plan = await projections.fetch_current_plan(self._db, task_id)
        if plan is None:
            raise TaskNotFound(task_id)
        records = await self._registry.list()
        agent_urls = {
            record.name: self._registry.agent_url(record) for record in records
        }
        dag_nodes = []
        for node in added_nodes:
            materialized = draft_to_dag(
                PlanDraft(rationale=rationale, nodes=[node]), agent_urls
            )["nodes"][0]
            materialized["derived"] = True
            dag_nodes.append(materialized)
        await self._events.append(
            task_id,
            EventType.PLAN_EXTENDED,
            {
                "plan_id": plan.id,
                "version": plan.version,
                "rationale": rationale,
                "added_nodes": dag_nodes,
                "added_edges": [
                    {"from": source, "to": target} for source, target in edges
                ],
            },
        )
        refreshed = await projections.fetch_current_plan(self._db, task_id)
        assert refreshed is not None
        return refreshed

    async def create_checkpoint(self, task_id: str) -> Checkpoint:
        task = await projections.fetch_task(self._db, task_id)
        plan = await projections.fetch_current_plan(self._db, task_id)
        if task is None or plan is None:
            raise TaskNotFound(task_id)
        nodes = await projections.fetch_nodes(self._db, task_id, plan.id)
        frontier = [node.id for node in nodes if node.status is NodeStatus.COMPLETED]
        artifacts = {node.id: node.output for node in nodes if node.output}
        checkpoint_id = uuid4().hex
        seq = await self._events.latest_seq(task_id)
        await self._events.append(
            task_id,
            EventType.CHECKPOINT_CREATED,
            {
                "checkpoint_id": checkpoint_id,
                "seq": seq,
                "plan_version": plan.version,
                "frontier": frontier,
                "artifacts": artifacts,
            },
        )
        return Checkpoint(
            id=checkpoint_id,
            task_id=task_id,
            seq=seq,
            plan_version=plan.version,
            frontier=frontier,
            artifacts=artifacts,
            created_at=datetime.now(UTC),
        )

    async def retry_node(self, task_id: str, node_id: str) -> Node:
        node = await projections.fetch_node(self._db, node_id)
        if node is None or node.task_id != task_id:
            raise TaskNotFound(f"node not found: {node_id}")
        if node.status is not NodeStatus.FAILED:
            raise ValueError(f"node {node_id} is {node.status.value}")
        task = await projections.fetch_task(self._db, task_id)
        if task is None:
            raise TaskNotFound(task_id)
        if task.status is TaskStatus.FAILED:
            await self._events.append(
                task_id,
                EventType.TASK_STATE_CHANGED,
                {
                    "from": TaskStatus.FAILED.value,
                    "to": TaskStatus.RUNNING.value,
                },
            )
        await self._events.append(
            task_id,
            EventType.NODE_STATE_CHANGED,
            {
                "node_id": node_id,
                "from": node.status.value,
                "to": NodeStatus.READY.value,
            },
        )
        refreshed = await projections.fetch_node(self._db, node_id)
        assert refreshed is not None
        return refreshed

    async def mark_running(self, task_id: str) -> OrchestrationTask:
        task = await projections.fetch_task(self._db, task_id)
        if task is None:
            raise TaskNotFound(task_id)
        if task.status is TaskStatus.PLANNING:
            await self._events.append(
                task_id,
                EventType.TASK_STATE_CHANGED,
                {"from": TaskStatus.PLANNING.value, "to": TaskStatus.RUNNING.value},
            )
            refreshed = await projections.fetch_task(self._db, task_id)
            assert refreshed is not None
            return refreshed
        return task

    async def get_snapshot(self, task_id: str) -> TaskSnapshot:
        task = await projections.fetch_task(self._db, task_id)
        if task is None:
            raise TaskNotFound(task_id)
        plan = await projections.fetch_current_plan(self._db, task_id)
        nodes = await projections.fetch_nodes(
            self._db, task_id, plan.id if plan else None
        )
        return TaskSnapshot(
            task=task,
            plan=plan,
            nodes=nodes,
            last_seq=await self._events.latest_seq(task_id),
        )
    async def finalize_if_complete(self, task_id: str) -> OrchestrationTask:
        task = await projections.fetch_task(self._db, task_id)
        if task is None:
            raise TaskNotFound(task_id)
        plan = await projections.fetch_current_plan(self._db, task_id)
        if task.status is not TaskStatus.RUNNING or plan is None:
            return task
        nodes = await projections.fetch_nodes(self._db, task_id, plan.id)
        nodes = [node for node in nodes if node.status is not NodeStatus.INVALIDATED]
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
