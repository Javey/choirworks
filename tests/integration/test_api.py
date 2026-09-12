import asyncio

import httpx
import pytest

from agent_hub.api.app import create_app
from agent_hub.config import Settings
from agent_hub.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM


@pytest.fixture
async def api(tmp_path, echo_agent):
    settings = Settings(store={"db_path": tmp_path / "api.db"})
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client, app, echo_agent.url


async def test_end_to_end_single_node(api):
    client, app, agent_url = api

    resp = await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    assert resp.status_code == 201
    assert resp.json()["name"] == "echo"

    resp = await client.post(
        "/v1/tasks", json={"request": "hi", "target": {"agent_name": "echo"}}
    )
    assert resp.status_code == 201
    created = resp.json()

    resp = await client.post(
        f"/v1/tasks/{created['task_id']}/nodes/{created['node_ids'][0]}/dispatch"
    )
    assert resp.status_code == 200
    snapshot = resp.json()
    assert snapshot["task"]["status"] == "completed"
    assert snapshot["nodes"][0]["status"] == "completed"
    assert snapshot["nodes"][0]["output"]["artifacts"][0]["text"] == "echo:hi"

    cursor = await app.state.db.conn.execute(
        "SELECT type FROM events WHERE task_id = ? ORDER BY seq", (created["task_id"],)
    )
    types = [row["type"] for row in await cursor.fetchall()]
    assert types[0] == "task.created"
    assert "node.dispatched" in types
    assert types[-1] == "task.completed"


async def test_unknown_agent_returns_400(api):
    client, _, _ = api
    resp = await client.post(
        "/v1/tasks", json={"request": "x", "target": {"agent_name": "ghost"}}
    )
    assert resp.status_code == 400


async def test_duplicate_agent_returns_409(api):
    client, _, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    resp = await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    assert resp.status_code == 409


async def test_missing_task_returns_404(api):
    client, _, _ = api
    resp = await client.get("/v1/tasks/nope")
    assert resp.status_code == 404


@pytest.fixture
async def api_auto(tmp_path, echo_agent):
    plan = PlanDraft(
        rationale="auto",
        nodes=[
            PlanNodeDraft(id="n1", name="echo", agent_name="echo", input={"text": "hi"})
        ],
    )
    llm = FakeLLM(structured_results=[plan])
    settings = Settings(
        store={"db_path": tmp_path / "auto.db"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=llm)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client, app, echo_agent.url


async def test_planner_path_runs_to_completion(api_auto):
    client, _, agent_url = api_auto
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    resp = await client.post("/v1/tasks", json={"request": "hi"})
    assert resp.status_code == 201
    task_id = resp.json()["task_id"]

    snapshot = None
    for _ in range(200):
        snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
        if snapshot["task"]["status"] == "completed":
            break
        await asyncio.sleep(0.05)
    assert snapshot is not None
    assert snapshot["task"]["status"] == "completed"
    assert snapshot["nodes"][0]["output"]["artifacts"][0]["text"] == "echo:hi"
