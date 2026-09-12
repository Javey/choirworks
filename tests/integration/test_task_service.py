import pytest

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.tasks import (
    CreatedTask,
    TargetSpec,
    TaskNotFound,
    TaskService,
    UnknownAgent,
)
from agent_hub.models.enums import NodeStatus, TaskStatus
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
