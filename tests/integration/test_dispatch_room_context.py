from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.registry import AgentRegistry
from choirworks.core.dispatcher import NodeDispatcher
from choirworks.core.room import post_message
from choirworks.core.tasks import TargetSpec, TaskService
from choirworks.models.enums import EventType, NodeStatus
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore


async def test_dispatch_includes_room_context(tmp_path, echo_agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("fake", echo_agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=30.0)
    try:
        created = await tasks.create_task("hi", TargetSpec(agent_name="fake"))
        assert created.conversation_id
        message = await post_message(
            db,
            events,
            conversation_id=created.conversation_id,
            role="user",
            sender="CEO",
            text="群里的关键上下文",
        )
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.status is NodeStatus.COMPLETED
        assert node.output is not None
        text = node.output["artifacts"][0]["text"]
        assert text.startswith("echo:")
        assert "群里的关键上下文" in text

        replayed = await events.replay(created.task_id)
        intent = next(
            event for event in replayed if event.type is EventType.NODE_DISPATCH_INTENT
        )
        assert intent.payload["context_included"] == [message.id]
    finally:
        await remote.close()
        await db.close()


async def test_dispatch_without_room_messages_keeps_legacy_text(tmp_path, echo_agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("fake", echo_agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=30.0)
    try:
        created = await tasks.create_task("hi", TargetSpec(agent_name="fake"))
        node = await dispatcher.dispatch_node(created.task_id, created.node_ids[0])
        assert node.output is not None
        assert node.output["artifacts"][0]["text"] == "echo:hi"
    finally:
        await remote.close()
        await db.close()
