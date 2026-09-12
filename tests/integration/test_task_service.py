import pytest

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.planner import PlanDraft, PlanNodeDraft
from agent_hub.core.tasks import (
    CreatedTask,
    TargetSpec,
    TaskNotFound,
    TaskService,
    UnknownAgent,
)
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


async def make_service(tmp_path, echo_agent) -> tuple[Database, TaskService]:
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("echo", echo_agent.url)
    return db, TaskService(db, EventStore(db), registry)


async def test_create_task_materializes_single_node(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        created = await service.create_task("做一件事", TargetSpec(agent_name="echo"))
        assert isinstance(created, CreatedTask)
        assert created.node_ids == [f"{created.plan_id}:n1"]

        snapshot = await service.get_snapshot(created.task_id)
        assert snapshot.task.status is TaskStatus.RUNNING
        assert snapshot.plan is not None and snapshot.plan.version == 1
        assert len(snapshot.nodes) == 1
        node = snapshot.nodes[0]
        assert node.status is NodeStatus.PENDING
        assert node.input == {"text": "做一件事"}
    finally:
        await db.close()


async def test_unknown_agent_rejected(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        with pytest.raises(UnknownAgent):
            await service.create_task("x", TargetSpec(agent_name="missing"))
        with pytest.raises(TaskNotFound):
            await service.get_snapshot("nope")
    finally:
        await db.close()


async def test_create_pending_task_and_materialize_draft(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        task_id = await service.create_pending_task("写一份报告")
        task = await service.get_snapshot(task_id)
        assert task.task.status is TaskStatus.PLANNING
        assert task.plan is None

        draft = PlanDraft(
            rationale="one step",
            nodes=[
                PlanNodeDraft(
                    id="n1", name="写", agent_name="echo", input={"text": "写一份报告"}
                )
            ],
        )
        created = await service.create_plan_from_draft(task_id, draft, version=1)
        await service.mark_running(task_id)
        snapshot = await service.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.RUNNING
        assert snapshot.plan is not None and snapshot.plan.version == 1
        assert snapshot.nodes[0].agent_url == echo_agent.url
        assert created.node_ids == [f"{created.plan_id}:n1"]
    finally:
        await db.close()


async def test_replan_emits_superseded_and_bumps_version(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        task_id = await service.create_pending_task("x")
        draft1 = PlanDraft(
            rationale="v1",
            nodes=[
                PlanNodeDraft(id="n1", name="a", agent_name="echo", input={"text": "x"})
            ],
        )
        await service.create_plan_from_draft(task_id, draft1, version=1)
        await service.mark_running(task_id)
        draft2 = PlanDraft(
            rationale="v2",
            nodes=[
                PlanNodeDraft(id="n1", name="b", agent_name="echo", input={"text": "x"})
            ],
        )
        await service.create_plan_from_draft(task_id, draft2, version=2)
        snapshot = await service.get_snapshot(task_id)
        assert snapshot.plan is not None and snapshot.plan.version == 2
        assert snapshot.plan.rationale == "v2"
        assert snapshot.nodes[0].name == "b"

        events = await EventStore(db).replay(task_id)
        assert EventType.PLAN_SUPERSEDED in [event.type for event in events]
    finally:
        await db.close()
