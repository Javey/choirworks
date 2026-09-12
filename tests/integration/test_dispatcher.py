import pytest

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.dispatcher import InvalidNodeState, NodeDispatcher
from agent_hub.core.tasks import TargetSpec, TaskService
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.fake_agents.echo_agent import start_fake_agent


async def setup(tmp_path, agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("fake", agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=30.0)
    return db, remote, events, tasks, dispatcher


async def test_dispatch_echo_completes_task(tmp_path, echo_agent):
    db, remote, events, tasks, dispatcher = await setup(tmp_path, echo_agent)
    try:
        created = await tasks.create_task("hi", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.COMPLETED
        assert node.output is not None
        assert node.output["artifacts"][0]["text"] == "echo:hi"
        assert node.a2a_task_id

        task = await tasks.finalize_if_complete(created.task_id)
        assert task.status is TaskStatus.COMPLETED

        types = [e.type for e in await events.replay(created.task_id)]
        assert types[0] is EventType.TASK_CREATED
        assert EventType.NODE_DISPATCHED in types
        assert EventType.NODE_OUTPUT in types
        assert types[-1] is EventType.TASK_COMPLETED
    finally:
        await remote.close()
        await db.close()


async def test_dispatch_input_required_stops_at_input_required(tmp_path):
    agent = await start_fake_agent("ask")
    db, remote, events, tasks, dispatcher = await setup(tmp_path, agent)
    try:
        created = await tasks.create_task("ask", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.INPUT_REQUIRED
        task = await tasks.finalize_if_complete(created.task_id)
        assert task.status is TaskStatus.RUNNING
    finally:
        await remote.close()
        await db.close()
        await agent.stop()


async def test_dispatch_failure_marks_node_and_task_failed(tmp_path):
    agent = await start_fake_agent("fail")
    db, remote, events, tasks, dispatcher = await setup(tmp_path, agent)
    try:
        created = await tasks.create_task("fail", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.FAILED
        task = await tasks.finalize_if_complete(created.task_id)
        assert task.status is TaskStatus.FAILED
    finally:
        await remote.close()
        await db.close()
        await agent.stop()


async def test_dispatch_timeout(tmp_path):
    agent = await start_fake_agent("slow")
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("fake", agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=0.5)
    try:
        created = await tasks.create_task("slow", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.FAILED
        assert node.error and "timed out" in node.error
    finally:
        await remote.close()
        await db.close()
        await agent.stop()


async def test_dispatch_twice_rejected(tmp_path, echo_agent):
    db, remote, events, tasks, dispatcher = await setup(tmp_path, echo_agent)
    try:
        created = await tasks.create_task("hi", TargetSpec(agent_name="fake"))
        await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        with pytest.raises(InvalidNodeState):
            await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
    finally:
        await remote.close()
        await db.close()
