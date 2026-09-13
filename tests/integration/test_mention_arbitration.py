import asyncio

import httpx
import pytest

from agent_hub.api.app import create_app
from agent_hub.config import Settings


@pytest.fixture
async def api(tmp_path, echo_agent):
    settings = Settings(
        store={"db_path": tmp_path / "mentions.db"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client, app, echo_agent.url


async def make_conversation(app) -> str:
    task_id = await app.state.task_service.create_pending_task("群聊测试")
    snapshot = await app.state.task_service.get_snapshot(task_id)
    assert snapshot.task.conversation_id
    return snapshot.task.conversation_id


async def wait_completed(client, task_id: str) -> dict:
    snapshot = None
    for _ in range(300):
        snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
        if snapshot["task"]["status"] == "completed":
            return snapshot
        await asyncio.sleep(0.05)
    raise AssertionError(f"task not completed: {snapshot}")


async def test_agent_mention_creates_helper_node_and_joins(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    await client.post("/v1/agents", json={"name": "analyst", "card_url": agent_url})
    conversation_id = await make_conversation(app)

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={"text": "请 @analyst 帮忙分析", "mentions": ["echo"]},
    )
    task_id = resp.json()["task_id"]
    snapshot = await wait_completed(client, task_id)

    derived = [
        node for node in snapshot["plan"]["dag"]["nodes"] if node.get("derived")
    ]
    assert any(node["agent_name"] == "analyst" for node in derived)
    analyst_nodes = [
        node for node in snapshot["nodes"] if node["agent_name"] == "analyst"
    ]
    assert analyst_nodes and all(node["status"] == "completed" for node in analyst_nodes)

    timeline = (await client.get(f"/v1/conversations/{conversation_id}/messages")).json()
    assert any(
        message["role"] == "assistant" and "已加入工作" in message["text"]
        for message in timeline["messages"]
    )
    assert "analyst" in {
        member["agent_name"] for member in timeline["members"]
    }
    assert "analyst" in {
        message["sender"]
        for message in timeline["messages"]
        if message["role"] == "agent"
    }


async def test_self_mention_is_ignored(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    conversation_id = await make_conversation(app)

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={"text": "@echo 自己处理", "mentions": ["echo"]},
    )
    snapshot = await wait_completed(client, resp.json()["task_id"])
    derived = [
        node for node in snapshot["plan"]["dag"]["nodes"] if node.get("derived")
    ]
    assert derived == []
