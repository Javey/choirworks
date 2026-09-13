import asyncio
import contextlib
import json

import httpx
import pytest
import uvicorn

from agent_hub.api.app import create_app
from agent_hub.config import Settings
from agent_hub.core.planner import PlanDraft, PlanNodeDraft
from agent_hub.sim.ports import free_port
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
        store={"db_path": tmp_path / "messages.db"},
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


async def test_post_message_creates_task_and_timeline(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    conversation_id = await make_conversation(app)

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages", json={"text": "hi"}
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["seq"] == 1
    assert body["task_id"]
    await wait_completed(client, body["task_id"])

    timeline = (await client.get(f"/v1/conversations/{conversation_id}/messages")).json()
    assert [message["role"] for message in timeline["messages"]] == ["user"]
    assert timeline["messages"][0]["text"] == "hi"
    assert timeline["last_seq"] == 1

    page = (
        await client.get(f"/v1/conversations/{conversation_id}/messages?since_seq=1")
    ).json()
    assert page["messages"] == []


async def test_post_message_validates_input(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    conversation_id = await make_conversation(app)

    quote = await client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={"text": "hi", "quote_id": "m1"},
    )
    assert quote.status_code == 400
    unknown = await client.post(
        f"/v1/conversations/{conversation_id}/messages",
        json={"text": "hi", "mentions": ["ghost"]},
    )
    assert unknown.status_code == 400
    missing = await client.post("/v1/conversations/missing/messages", json={"text": "hi"})
    assert missing.status_code == 404


async def test_conversation_sse_replays_room_and_task_events(tmp_path, echo_agent):
    settings = Settings(
        store={"db_path": tmp_path / "room-sse.db"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=make_llm())
    port = free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - 轮询 uvicorn 启动状态
        await asyncio.sleep(0.02)
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", timeout=10.0
        ) as client:
            await client.post(
                "/v1/agents", json={"name": "echo", "card_url": echo_agent.url}
            )
            task_id = await app.state.task_service.create_pending_task("群聊测试")
            snapshot = await app.state.task_service.get_snapshot(task_id)
            conversation_id = snapshot.task.conversation_id
            resp = await client.post(
                f"/v1/conversations/{conversation_id}/messages", json={"text": "hi"}
            )
            message_task_id = resp.json()["task_id"]

            events: list[dict] = []

            async def collect() -> None:
                async with client.stream(
                    "GET", f"/v1/conversations/{conversation_id}/stream?since_seq=0"
                ) as response:
                    assert response.status_code == 200
                    current: dict = {}
                    async for line in response.aiter_lines():
                        if line == "":
                            if current:
                                events.append(current)
                                if current.get("event") == "task.completed":
                                    return
                                current = {}
                            continue
                        if line.startswith("id: "):
                            current["id"] = int(line[4:])
                        elif line.startswith("event: "):
                            current["event"] = line[7:]
                        elif line.startswith("data: "):
                            current["data"] = json.loads(line[6:])

            await asyncio.wait_for(collect(), timeout=10.0)
            types = [event["event"] for event in events]
            assert "message.posted" in types
            assert "node.dispatched" in types
            assert "task.completed" in types
            assert message_task_id
    finally:
        server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, timeout=5)
