from a2a.types import TaskState

from choirworks.a2a.client import RemoteAgentClient
from choirworks.sim.fake_agent import start_fake_agent


async def test_resolve_card_and_send_text(echo_agent):
    client = RemoteAgentClient()
    try:
        card = await client.resolve_card(echo_agent.url)
        assert card.name == "echo"

        chunks = [chunk async for chunk in client.send_text(echo_agent.url, "hello")]
        assert chunks[0].HasField("task")

        states = [
            chunk.status_update.status.state
            for chunk in chunks
            if chunk.HasField("status_update")
        ]
        assert TaskState.TASK_STATE_COMPLETED in states

        task = chunks[0].task
        assert task.artifacts or any(c.HasField("artifact_update") for c in chunks)
    finally:
        await client.close()


async def test_send_text_sets_deterministic_message_id(echo_agent):
    client = RemoteAgentClient()
    try:
        chunks = [
            chunk
            async for chunk in client.send_text(
                echo_agent.url,
                "hi",
                context_id="ctx1",
                message_id="t1:n1:1",
            )
        ]
        assert chunks[0].task.context_id == "ctx1"
        assert chunks[0].task.id
    finally:
        await client.close()


async def test_get_and_subscribe_task():
    agent = await start_fake_agent("delay")
    client = RemoteAgentClient()
    try:
        stream = client.send_text(agent.url, "hello")
        first = await anext(stream)
        remote_task_id = first.task.id

        resumed = [
            c async for c in client.subscribe_task(agent.url, remote_task_id)
        ]
        states = [
            c.status_update.status.state
            for c in resumed
            if c.HasField("status_update")
        ]
        assert TaskState.TASK_STATE_COMPLETED in states

        task = await client.get_task(agent.url, remote_task_id)
        assert task is not None
        assert task.status.state == TaskState.TASK_STATE_COMPLETED
    finally:
        await client.close()
        await agent.stop()


async def test_cancel_task(ask_agent):
    client = RemoteAgentClient()
    try:
        chunks = [c async for c in client.send_text(ask_agent.url, "ask")]
        remote_task_id = chunks[0].task.id
        assert remote_task_id
        await client.cancel_task(ask_agent.url, remote_task_id)
        task = await client.get_task(ask_agent.url, remote_task_id)
        assert task is not None
        assert task.status.state in (
            TaskState.TASK_STATE_CANCELED,
            TaskState.TASK_STATE_INPUT_REQUIRED,
        )
    finally:
        await client.close()
