import httpx
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.helpers import get_artifact_text, new_text_message
from a2a.types import Role, SendMessageRequest, TaskState

from choirworks.sim.fake_agent import start_fake_agent


async def test_fake_agent_echoes_with_official_client():
    agent = await start_fake_agent("echo")
    try:
        async with httpx.AsyncClient() as http:
            resolver = A2ACardResolver(httpx_client=http, base_url=agent.url)
            card = await resolver.get_agent_card()
            assert card.name == "fake-echo"

        client = await create_client(
            agent=agent.card, client_config=ClientConfig(streaming=True)
        )
        request = SendMessageRequest(message=new_text_message("hi", role=Role.ROLE_USER))
        task_id = None
        states = []
        artifact_texts = []
        async for chunk in client.send_message(request):
            if chunk.HasField("task"):
                task_id = chunk.task.id
            elif chunk.HasField("status_update"):
                states.append(chunk.status_update.status.state)
            elif chunk.HasField("artifact_update"):
                artifact_texts.append(get_artifact_text(chunk.artifact_update.artifact))
        assert task_id
        assert TaskState.TASK_STATE_COMPLETED in states
        assert "echo:hi" in artifact_texts
        await client.close()
    finally:
        await agent.stop()


async def test_fake_agent_streams_artifact_chunks():
    agent = await start_fake_agent("write", chunk_size=3, chunk_delay=0.0)
    try:
        client = await create_client(
            agent=agent.card, client_config=ClientConfig(streaming=True)
        )
        request = SendMessageRequest(message=new_text_message("hi", role=Role.ROLE_USER))
        texts: list[str] = []
        appends: list[bool] = []
        last_chunks: list[bool] = []
        async for chunk in client.send_message(request):
            if chunk.HasField("artifact_update"):
                update = chunk.artifact_update
                texts.append(get_artifact_text(update.artifact))
                appends.append(bool(update.append))
                last_chunks.append(bool(update.last_chunk))
        assert len(texts) >= 3
        assert "".join(texts) == "文稿（writer）：基于「hi」生成的模拟报告。"
        assert appends[0] is False
        assert all(appends[1:])
        assert last_chunks[-1] is True
        assert not any(last_chunks[:-1])
        await client.close()
    finally:
        await agent.stop()
