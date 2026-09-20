import pytest

from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.registry import AgentRegistry, DuplicateAgentName
from choirworks.store.db import Database


async def test_register_list_refresh_delete(tmp_path, echo_agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    try:
        record = await registry.register("echo", echo_agent.url)
        assert record.name == "echo"
        assert registry.agent_url(record) == echo_agent.url

        listed = await registry.list()
        assert [r.name for r in listed] == ["echo"]

        by_name = await registry.get_by_name("echo")
        assert by_name is not None and by_name.id == record.id

        refreshed = await registry.refresh(record.id)
        assert refreshed.card["name"] == "echo"

        assert await registry.delete(record.id) is True
        assert await registry.list() == []
    finally:
        await remote.close()
        await db.close()


async def test_duplicate_name_rejected(tmp_path, echo_agent):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    try:
        await registry.register("echo", echo_agent.url)
        with pytest.raises(DuplicateAgentName):
            await registry.register("echo", echo_agent.url)
    finally:
        await remote.close()
        await db.close()


async def test_registration_survives_application_restart(tmp_path, echo_agent):
    from tests.support.sdk import sdk_hub

    async with sdk_hub(tmp_path, "persistent-registry.db") as (_, http, _):
        registered = await http.post(
            "/v1/agents", json={"name": "persisted", "card_url": echo_agent.url}
        )
        assert registered.status_code == 201
        record = registered.json()

    async with sdk_hub(tmp_path, "persistent-registry.db") as (_, http, _):
        assert (await http.get("/v1/agents")).json() == [record]
        refreshed = await http.post(f"/v1/agents/{record['id']}/refresh")
        assert refreshed.status_code == 200
        assert refreshed.json()["id"] == record["id"]
