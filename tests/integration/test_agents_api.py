import asyncio
import json
import sqlite3
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.registry import AgentRegistry
from choirworks.api.agents import router
from choirworks.store.db import Database


@pytest.fixture
async def agents_api(tmp_path):
    state = SimpleNamespace(
        card={
            "name": "Remote worker",
            "description": "Research specialist",
            "version": "1.0.0",
            "capabilities": {"streaming": True},
            "defaultInputModes": ["text/plain"],
            "defaultOutputModes": ["text/plain"],
            "skills": [{"id": "search", "name": "Search", "description": "Search documents"}],
            "supportedInterfaces": [
                {
                    "url": "http://agent/rpc-v1",
                    "protocolBinding": "JSONRPC",
                    "protocolVersion": "1.0",
                }
            ],
        },
        failure=None,
        requests=[],
        gate=None,
        card_requests=0,
    )

    async def respond(request):
        state.requests.append(str(request.url))
        if request.method == "GET":
            state.card_requests += 1
            if state.gate is not None:
                if state.card_requests == 2:
                    state.gate.set()
                await state.gate.wait()
            if state.failure == "timeout":
                raise httpx.ReadTimeout("agent timed out", request=request)
            if state.failure == "offline":
                raise httpx.ConnectError("agent unavailable", request=request)
            if state.failure == "json":
                return httpx.Response(200, text="not JSON")
            return httpx.Response(200, json=state.card)
        rpc = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "jsonrpc": "2.0",
                "id": rpc["id"],
                "result": {
                    "id": "remote-task",
                    "contextId": "ctx",
                    "status": {"state": "TASK_STATE_COMPLETED"},
                },
            },
        )

    db = Database(tmp_path / "registry.db")
    await db.initialize()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as remote_http:
        remote = RemoteAgentClient(remote_http)
        app = FastAPI()
        app.include_router(router, prefix="/v1")
        app.state.registry = AgentRegistry(db, remote)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://hub"
        ) as http:
            try:
                yield SimpleNamespace(http=http, db=db, remote=remote, state=state, app=app)
            finally:
                await remote.close()
                await db.close()


async def register(env, name="worker", url="http://agent"):
    return await env.http.post("/v1/agents", json={"name": name, "card_url": url})


async def test_register_normalizes_and_persists_across_database_reopen(agents_api):
    env = agents_api
    response = await register(env, " worker ", "http://agent/")
    assert response.status_code == 201
    record = response.json()
    assert record["name"] == "worker"
    assert record["card_url"] == "http://agent"
    assert record["card"]["skills"][0]["id"] == "search"
    assert record["health"] == "ok"
    assert record["last_seen"] and record["created_at"]
    await env.db.close()
    await env.db.initialize()
    env.app.state.registry = AgentRegistry(env.db, env.remote)
    assert (await env.http.get("/v1/agents")).json() == [record]
    assert (await register(env, "worker")).status_code == 409
    assert (await env.http.delete(f"/v1/agents/{record['id']}")).status_code == 204
    assert (await env.http.get("/v1/agents")).json() == []
    assert (await env.http.delete(f"/v1/agents/{record['id']}")).status_code == 404


@pytest.mark.parametrize("name", ["", "  ", "two words", "@worker", "bad/name", "a" * 65])
async def test_invalid_name_rejected_before_discovery(agents_api, name):
    assert (await register(agents_api, name)).status_code == 422
    assert agents_api.state.requests == []


@pytest.mark.parametrize(
    "url",
    [
        "",
        "agent",
        "ftp://agent",
        "file:///tmp/card",
        "http://u:p@agent",
        "http://agent?q=x",
        "http://agent/#x",
    ],
)
async def test_invalid_base_url_rejected_before_discovery(agents_api, url):
    assert (await register(agents_api, url=url)).status_code == 422
    assert agents_api.state.requests == []


@pytest.mark.parametrize("failure", ["offline", "timeout", "json"])
async def test_discovery_failure_does_not_create_record(agents_api, failure):
    agents_api.state.failure = failure
    response = await register(agents_api)
    assert response.status_code == 400
    assert "failed to resolve agent card" in response.json()["detail"]
    assert (await agents_api.http.get("/v1/agents")).json() == []


@pytest.mark.parametrize(
    "card",
    [
        {},
        [],
        {"name": "worker"},
        {
            "name": "worker",
            "supportedInterfaces": [
                {"url": "http://agent", "protocolBinding": "JSONRPC", "protocolVersion": "0.3"},
            ],
        },
    ],
)
async def test_invalid_or_incompatible_card_rejected(agents_api, card):
    agents_api.state.card = card
    assert (await register(agents_api)).status_code == 400
    assert (await agents_api.http.get("/v1/agents")).json() == []


async def test_concurrent_duplicate_registration_returns_conflict(agents_api):
    agents_api.state.gate = asyncio.Event()
    async with asyncio.timeout(5):
        responses = await asyncio.gather(register(agents_api), register(agents_api))
    assert sorted(response.status_code for response in responses) == [201, 409]
    assert len((await agents_api.http.get("/v1/agents")).json()) == 1


async def test_refresh_failure_preserves_card_and_last_seen_then_recovers(agents_api):
    env = agents_api
    record = (await register(env)).json()
    endpoint = f"/v1/agents/{record['id']}/refresh"
    env.state.failure = "offline"
    response = await env.http.post(endpoint)
    assert response.status_code == 502
    failed = (await env.http.get("/v1/agents")).json()[0]
    assert failed["health"] == "unavailable"
    assert failed["card"] == record["card"]
    assert failed["last_seen"] == record["last_seen"]
    env.state.failure = None
    env.state.card["skills"][0]["id"] = "new-skill"
    refreshed = (await env.http.post(endpoint)).json()
    assert refreshed["health"] == "ok"
    assert refreshed["id"] == record["id"]
    assert refreshed["created_at"] == record["created_at"]
    assert refreshed["last_seen"] >= record["last_seen"]
    assert refreshed["card"]["skills"][0]["id"] == "new-skill"


async def test_refresh_uses_new_endpoint_without_closing_shared_http_pool(agents_api):
    env = agents_api
    record = (await register(env)).json()
    assert await env.remote.get_task("http://agent", "remote-task") is not None
    assert env.state.requests[-1] == "http://agent/rpc-v1"
    env.state.card["supportedInterfaces"][0]["url"] = "http://agent/rpc-v2"
    assert (await env.http.post(f"/v1/agents/{record['id']}/refresh")).status_code == 200
    assert await env.remote.get_task("http://agent", "remote-task") is not None
    assert env.state.requests[-1] == "http://agent/rpc-v2"
    # New calls use the refreshed cached card, without another discovery request.
    assert env.state.card_requests == 2


async def test_refresh_missing_agent_does_not_attempt_discovery(agents_api):
    response = await agents_api.http.post("/v1/agents/missing/refresh")
    assert response.status_code == 404
    assert response.json() == {"detail": "agent not found: missing"}
    assert agents_api.state.requests == []


async def test_refresh_racing_delete_does_not_resurrect_record(agents_api, monkeypatch):
    env = agents_api
    record = (await register(env)).json()
    original = env.remote.resolve_card

    async def resolve_after_delete(url):
        await env.app.state.registry.delete(record["id"])
        return await original(url)

    monkeypatch.setattr(env.remote, "resolve_card", resolve_after_delete)
    assert (await env.http.post(f"/v1/agents/{record['id']}/refresh")).status_code == 404
    assert (await env.http.get("/v1/agents")).json() == []


async def test_registry_list_sorted_by_name(agents_api):
    assert (await register(agents_api, "z-worker")).status_code == 201
    assert (await register(agents_api, "a-worker")).status_code == 201
    assert [item["name"] for item in (await agents_api.http.get("/v1/agents")).json()] == [
        "a-worker",
        "z-worker",
    ]


async def test_storage_failure_is_not_reported_as_bad_agent_card(agents_api, monkeypatch):
    async def fail_storage(*args):
        raise sqlite3.OperationalError("database is unavailable")

    monkeypatch.setattr(agents_api.app.state.registry, "register", fail_storage)
    with pytest.raises(sqlite3.OperationalError):
        await register(agents_api)
