import asyncio

import httpx
import pytest

from agent_hub.api.app import create_app
from agent_hub.config import Settings
from agent_hub.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM


def make_llm() -> FakeLLM:
    plan = PlanDraft(
        rationale="room",
        nodes=[
            PlanNodeDraft(id="n1", name="echo", agent_name="echo", input={"text": "hi"})
        ],
    )
    return FakeLLM(structured_results=[plan])


@pytest.fixture
async def api(tmp_path, echo_agent):
    settings = Settings(
        store={"db_path": tmp_path / "routing.db"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=make_llm())
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
    for _ in range(200):
        snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
        if snapshot["task"]["status"] == "completed":
            return snapshot
        await asyncio.sleep(0.05)
    raise AssertionError(f"task not completed: {snapshot}")


async def test_single_mention_creates_direct_agent_task(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    conversation_id = await make_conversation(app)

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={"text": "hi", "mentions": ["echo"]},
    )
    assert resp.status_code == 201
    body = resp.json()
    await wait_completed(client, body["task_id"])

    snapshot = (await client.get(f"/v1/tasks/{body['task_id']}")).json()
    assert snapshot["nodes"][0]["agent_name"] == "echo"

    timeline = (await client.get(f"/v1/conversations/{conversation_id}/messages")).json()
    user_messages = [
        message for message in timeline["messages"] if message["role"] == "user"
    ]
    assert len(user_messages) == 1
    assert user_messages[0]["mentions"] == ["echo"]
    assert user_messages[0]["task_id"] == body["task_id"]
    content_roles = [
        message["role"]
        for message in timeline["messages"]
        if message["role"] in {"user", "agent"}
    ]
    assert content_roles == ["user", "agent"]


async def test_multiple_mentions_join_members_and_announce(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    await client.post("/v1/agents", json={"name": "writer", "card_url": agent_url})
    conversation_id = await make_conversation(app)

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={"text": "hi", "mentions": ["echo", "writer"]},
    )
    assert resp.status_code == 201
    await wait_completed(client, resp.json()["task_id"])

    timeline = (await client.get(f"/v1/conversations/{conversation_id}/messages")).json()
    assert {member["agent_name"] for member in timeline["members"]} == {"echo", "writer"}
    join_notes = [
        message["text"]
        for message in timeline["messages"]
        if message["role"] == "assistant" and "加入群聊" in message["text"]
    ]
    assert len(join_notes) == 2


async def test_unknown_mention_rejected(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    conversation_id = await make_conversation(app)

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={"text": "hi", "mentions": ["ghost"]},
    )
    assert resp.status_code == 400
