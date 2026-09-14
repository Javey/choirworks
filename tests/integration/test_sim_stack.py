import asyncio

import httpx
import pytest

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.llm import LiteLLMClient
from choirworks.core.planner import PlanDraft  # noqa: F401  (import graph sanity)
from choirworks.sim.litellm_mock import sim_acompletion
from choirworks.sim.runner import start_sim_agents


@pytest.fixture
async def sim(tmp_path):
    agents = await start_sim_agents(chunk_size=3, chunk_delay=0.0)
    settings = Settings(
        store={"db_path": tmp_path / "sim.db"},
        policies={"overrides": [{"agent_name": "critic", "policy": "human"}]},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(
        settings,
        llm=LiteLLMClient(model="sim", completion_fn=sim_acompletion),
    )
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            for name, agent in agents:
                await app.state.registry.register(name, agent.url)
            yield client, app
    for _, agent in agents:
        await agent.stop()


async def wait_for_status(
    client, task_id: str, statuses: set[str], timeout_seconds: float = 25.0
):
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    snapshots = []
    while asyncio.get_event_loop().time() < deadline:
        response = await client.get(f"/v1/tasks/{task_id}")
        assert response.status_code == 200
        snapshot = response.json()
        snapshots.append(snapshot)
        if snapshot["task"]["status"] in statuses:
            return snapshot
        await asyncio.sleep(0.1)
    raise AssertionError(f"task {task_id} did not reach {statuses}: {snapshots[-1]}")


async def test_sim_research_then_write(sim):
    client, app = sim
    created = (
        await client.post("/v1/tasks", json={"request": "帮我调研 A2A 协议并写一份摘要"})
    ).json()
    snapshot = await wait_for_status(client, created["task_id"], {"completed"})
    assert snapshot["plan"]["version"] == 1
    outputs = {
        node["agent_name"]: (node["output"] or {}).get("artifacts", [{}])[0].get("text", "")
        for node in snapshot["nodes"]
    }
    assert "调研结果" in outputs["researcher"]
    assert "文稿" in outputs["writer"]

    cursor = await app.state.db.conn.execute(
        "SELECT payload FROM events WHERE task_id = ? AND type = 'node.artifact'"
        " ORDER BY seq",
        (created["task_id"],),
    )
    rows = await cursor.fetchall()
    assert len(rows) >= 3
    deltas = [row["payload"] for row in rows]
    assert all('"append": true' in delta or '"append": false' in delta for delta in deltas)
    assert any('"append": true' in delta for delta in deltas)


async def test_sim_review_triggers_human_intervention(sim):
    client, _ = sim
    created = (
        await client.post("/v1/tasks", json={"request": "帮我评审这段文案"})
    ).json()
    snapshot = await wait_for_status(client, created["task_id"], {"awaiting_input"})

    response = await client.get(
        f"/v1/tasks/{created['task_id']}/interventions?status=pending"
    )
    interventions = response.json()
    assert len(interventions) == 1
    assert "请确认" in interventions[0]["question"]["text"]

    response = await client.post(
        f"/v1/tasks/{created['task_id']}/interventions/{interventions[0]['id']}",
        json={"text": "同意，按此定稿"},
    )
    assert response.status_code == 200

    snapshot = await wait_for_status(client, created["task_id"], {"completed"})
    critic = next(node for node in snapshot["nodes"] if node["agent_name"] == "critic")
    assert "已按你的意见定稿" in critic["output"]["artifacts"][0]["text"]


async def test_sim_broken_agent_triggers_replan(sim):
    client, app = sim
    created = (
        await client.post("/v1/tasks", json={"request": "模拟失败并降级替换"})
    ).json()
    snapshot = await wait_for_status(client, created["task_id"], {"completed"})

    assert snapshot["plan"]["version"] == 2
    assert [node["agent_name"] for node in snapshot["nodes"]] == ["writer"]
    assert snapshot["nodes"][0]["status"] == "completed"

    cursor = await app.state.db.conn.execute(
        "SELECT type FROM events WHERE task_id = ? ORDER BY seq", (created["task_id"],)
    )
    types = [row["type"] for row in await cursor.fetchall()]
    assert "plan.superseded" in types
    assert "node.retry.scheduled" in types
