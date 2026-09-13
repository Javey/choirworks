import asyncio

import httpx
import pytest

from agent_hub.api.app import create_app
from agent_hub.config import Settings
from agent_hub.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM


@pytest.fixture
async def api(tmp_path, echo_agent):
    plan = PlanDraft(
        rationale="room",
        nodes=[
            PlanNodeDraft(id="n1", name="echo", agent_name="echo", input={"text": "hi"})
        ],
    )
    settings = Settings(
        store={"db_path": tmp_path / "flow.db"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=FakeLLM(structured_results=[plan]))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client, app, echo_agent.url


async def wait_completed(client, task_id: str) -> dict:
    snapshot = None
    for _ in range(200):
        snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
        if snapshot["task"]["status"] == "completed":
            return snapshot
        await asyncio.sleep(0.05)
    raise AssertionError(f"task not completed: {snapshot}")


async def test_agent_output_becomes_room_message(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    task_id = await app.state.task_service.create_pending_task("群聊测试")
    snapshot = await app.state.task_service.get_snapshot(task_id)
    conversation_id = snapshot.task.conversation_id

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages", json={"text": "hi"}
    )
    await wait_completed(client, resp.json()["task_id"])

    timeline = (await client.get(f"/v1/conversations/{conversation_id}/messages")).json()
    messages = timeline["messages"]
    content = [message for message in messages if message["role"] in {"user", "agent"}]
    assert [message["role"] for message in content] == ["user", "agent"]
    agent_message = content[1]
    assert agent_message["sender"] == "echo"
    assert agent_message["text"].startswith("echo:")
    assert "hi" in agent_message["text"]
    assert agent_message["node_id"]
    assert agent_message["seq"] > content[0]["seq"]
