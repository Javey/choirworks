import asyncio
import contextlib
import json

import httpx
import pytest
import uvicorn

from agent_hub.api.app import create_app
from agent_hub.config import Settings
from agent_hub.sim.ports import free_port


@pytest.fixture
async def api(tmp_path, echo_agent):
    settings = Settings(store={"db_path": tmp_path / "sse.db"})
    app = create_app(settings)
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
            yield client, app, echo_agent.url
    finally:
        server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, timeout=5)


async def read_until_completed(response, timeout_seconds=5.0):
    events: list[dict] = []

    async def _read():
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

    await asyncio.wait_for(_read(), timeout_seconds)
    return events


async def test_sse_replays_history(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    created = (
        await client.post(
            "/v1/tasks", json={"request": "hi", "target": {"agent_name": "echo"}}
        )
    ).json()
    await client.post(
        f"/v1/tasks/{created['task_id']}/nodes/{created['node_ids'][0]}/dispatch"
    )

    async with client.stream(
        "GET", f"/v1/tasks/{created['task_id']}/events?after_seq=0"
    ) as response:
        assert response.status_code == 200
        events = await read_until_completed(response)

    ids = [event["id"] for event in events]
    assert ids == sorted(ids)
    assert len(ids) == len(set(ids))
    assert events[0]["event"] == "task.created"
    assert events[-1]["event"] == "task.completed"
    assert any(event["event"] == "node.dispatched" for event in events)


async def test_sse_streams_live_events(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    created = (
        await client.post(
            "/v1/tasks", json={"request": "hi", "target": {"agent_name": "echo"}}
        )
    ).json()
    task_id = created["task_id"]
    node_id = created["node_ids"][0]
    after = await app.state.event_store.latest_seq(task_id)

    async with client.stream(
        "GET", f"/v1/tasks/{task_id}/events?after_seq={after}"
    ) as response:
        dispatch = asyncio.create_task(
            client.post(f"/v1/tasks/{task_id}/nodes/{node_id}/dispatch")
        )
        events = await read_until_completed(response)
        await dispatch

    assert events, "expected live events"
    assert all(event["id"] > after for event in events)
    assert events[-1]["event"] == "task.completed"


async def test_sse_resume_with_last_event_id(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    created = (
        await client.post(
            "/v1/tasks", json={"request": "hi", "target": {"agent_name": "echo"}}
        )
    ).json()
    task_id = created["task_id"]
    await client.post(f"/v1/tasks/{task_id}/nodes/{created['node_ids'][0]}/dispatch")
    first_seq = (await app.state.event_store.replay(task_id))[0].seq

    async with client.stream(
        "GET",
        f"/v1/tasks/{task_id}/events",
        headers={"Last-Event-ID": str(first_seq)},
    ) as response:
        assert response.status_code == 200
        events = await read_until_completed(response)

    assert events
    assert all(event["id"] > first_seq for event in events)


async def test_sse_unknown_task_returns_404(api):
    client, _, _ = api
    resp = await client.get("/v1/tasks/nope/events")
    assert resp.status_code == 404
