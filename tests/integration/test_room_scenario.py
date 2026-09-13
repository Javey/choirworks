import asyncio

import httpx
import pytest

from agent_hub.api.app import create_app
from agent_hub.config import Settings
from agent_hub.sim.fake_agent import start_fake_agent
from agent_hub.sim.llm import SimLLM


@pytest.fixture
async def scenario(tmp_path):
    researcher = await start_fake_agent("collaborate", name="researcher")
    analyst = await start_fake_agent("assist", name="analyst")
    try:
        settings = Settings(
            store={"db_path": tmp_path / "scenario.db"},
            policies={
                "overrides": [
                    {"agent_name": "researcher", "policy": "peer_agent"},
                ]
            },
            scheduler={"retry_backoff_seconds": 0.0},
        )
        app = create_app(settings, llm=SimLLM())
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                for name, agent in (("researcher", researcher), ("analyst", analyst)):
                    resp = await client.post(
                        "/v1/agents", json={"name": name, "card_url": agent.url}
                    )
                    assert resp.status_code == 201
                yield client, app
    finally:
        await researcher.stop()
        await analyst.stop()


async def wait_completed(client, task_id: str, timeout_seconds: float = 15.0):
    snapshot = None
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while asyncio.get_event_loop().time() < deadline:
        snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
        if snapshot["task"]["status"] == "completed":
            return snapshot
        await asyncio.sleep(0.05)
    raise AssertionError(f"task not completed: {snapshot}")


async def test_group_scenario_peer_assist_and_timeline(scenario):
    client, app = scenario
    created = (
        await client.post("/v1/tasks", json={"request": "群聊初始化"})
    ).json()
    conversation_id = created["conversation_id"]

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={
            "text": "请协调多个子代理协作完成这项分析",
            "mentions": ["researcher"],
        },
    )
    assert resp.status_code == 201
    task_id = resp.json()["task_id"]
    snapshot = await wait_completed(client, task_id)

    derived = [
        node for node in snapshot["plan"]["dag"]["nodes"] if node.get("derived")
    ]
    assert any(node["agent_name"] == "analyst" for node in derived)

    timeline = (await client.get(f"/v1/conversations/{conversation_id}/messages")).json()
    texts = [message["text"] for message in timeline["messages"]]
    assert any("请求 @analyst 协助" in text for text in texts)
    assert "analyst" in {member["agent_name"] for member in timeline["members"]}
    assert "analyst" in {
        message["sender"]
        for message in timeline["messages"]
        if message["role"] == "agent"
    }
    assert any(text.startswith("任务完成") for text in texts)


async def test_group_scenario_survives_rebuild(scenario):
    client, app = scenario
    created = (
        await client.post("/v1/tasks", json={"request": "群聊初始化"})
    ).json()
    conversation_id = created["conversation_id"]
    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={
            "text": "请协调多个子代理协作完成这项分析",
            "mentions": ["researcher"],
        },
    )
    await wait_completed(client, resp.json()["task_id"])
    before = (
        await client.get(f"/v1/conversations/{conversation_id}/messages")
    ).json()["messages"]

    from agent_hub.store import projections

    await projections.rebuild(app.state.db)
    after = (
        await client.get(f"/v1/conversations/{conversation_id}/messages")
    ).json()["messages"]
    assert [message["id"] for message in before] == [
        message["id"] for message in after
    ]
    assert [message["text"] for message in before] == [
        message["text"] for message in after
    ]
