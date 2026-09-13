import asyncio

import httpx
import pytest

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.sim.llm import SimLLM
from choirworks.sim.runner import start_sim_agents


@pytest.fixture
async def sim(tmp_path):
    agents = await start_sim_agents(chunk_size=4, chunk_delay=0.0)
    settings = Settings(
        store={"db_path": tmp_path / "sim.db"},
        policies={
            "overrides": [
                {"agent_name": "critic", "policy": "human"},
                {"agent_name": "researcher", "policy": "peer_agent"},
                {"agent_name": "writer", "policy": "peer_agent"},
            ]
        },
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=SimLLM())
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            for name, agent in agents:
                await app.state.registry.register(name, agent.url)
            yield client, app
    for _, agent in agents:
        await agent.stop()


async def wait_for_status(client, task_id: str, statuses: set[str], timeout_seconds=30.0):
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    snapshot = None
    while asyncio.get_event_loop().time() < deadline:
        response = await client.get(f"/v1/tasks/{task_id}")
        assert response.status_code == 200
        snapshot = response.json()
        if snapshot["task"]["status"] in statuses:
            return snapshot
        await asyncio.sleep(0.1)
    raise AssertionError(f"task did not reach {statuses}: {snapshot}")


async def test_sim_coordination_builds_dynamic_dag(sim):
    client, app = sim
    created = (
        await client.post(
            "/v1/tasks", json={"request": "请协调多个子代理协作完成这项分析"}
        )
    ).json()
    task_id = created["task_id"]
    snapshot = await wait_for_status(client, task_id, {"completed"})

    dag_nodes = {node["id"]: node for node in snapshot["plan"]["dag"]["nodes"]}
    assert {"n1", "n2"} <= set(dag_nodes)
    assert len(dag_nodes) == 4
    assert sum(1 for node in dag_nodes.values() if node.get("derived")) == 2
    assert {node["agent_name"] for node in snapshot["nodes"]} >= {
        "researcher",
        "writer",
        "analyst",
    }

    all_nodes = await app.state.db.conn.execute(
        "SELECT nodes.agent_name, nodes.deps, nodes.status FROM nodes WHERE task_id = ?",
        (task_id,),
    )
    rows = await all_nodes.fetchall()
    assert len(rows) == 4
    derived = [row for row in rows if row["deps"] is not None and row["deps"] == "[]"]
    assert len(derived) == 2
    assert {row["status"] for row in rows} == {"completed"}

    response = await client.get(f"/v1/tasks/{task_id}/interventions")
    assert response.status_code == 200
    interventions = response.json()
    assert len(interventions) == 2
    assert all(item["status"] == "resolved" for item in interventions)
    responders = {item["responder"] for item in interventions}
    assert responders == {"analyst", "researcher"}
    assigned = {item["assigned_node_id"] for item in interventions}
    assert None not in assigned

    cursor = await app.state.db.conn.execute(
        "SELECT type, payload FROM events WHERE task_id = ? AND type = 'plan.extended'",
        (task_id,),
    )
    extended = await cursor.fetchall()
    assert len(extended) == 2

    parents = [row for row in rows if row["deps"] != "[]"]
    assert len(parents) == 2
    assert {row["agent_name"] for row in parents} == {"researcher", "writer"}
