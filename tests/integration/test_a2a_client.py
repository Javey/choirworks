from a2a.types import TaskState

from agent_hub.a2a.client import RemoteAgentClient


async def test_resolve_card_and_send_text(echo_agent):
    client = RemoteAgentClient()
    try:
        card = await client.resolve_card(echo_agent.url)
        assert card.name == "fake-echo"

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
