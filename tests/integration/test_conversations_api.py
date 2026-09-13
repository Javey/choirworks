import httpx
import pytest

from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.registry import AgentRegistry
from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.tasks import ConversationNotFound, TargetSpec, TaskService
from choirworks.store import projections
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore


async def make_service(tmp_path, echo_agent) -> tuple[Database, TaskService]:
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("echo", echo_agent.url)
    return db, TaskService(db, EventStore(db), registry)


async def test_create_pending_task_creates_conversation(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        task_id = await service.create_pending_task("第一问")
        snapshot = await service.get_snapshot(task_id)
        conversation_id = snapshot.task.conversation_id
        assert conversation_id
        conversation = await projections.fetch_conversation(db, conversation_id)
        assert conversation is not None and conversation.title == "第一问"
        summaries = await projections.fetch_conversation_summaries(db)
        assert len(summaries) == 1 and summaries[0].task_count == 1
    finally:
        await db.close()


async def test_follow_up_uses_existing_conversation(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        first = await service.create_pending_task("第一问")
        cid = (await service.get_snapshot(first)).task.conversation_id
        second = await service.create_pending_task("追问", conversation_id=cid)
        snapshot = await service.get_snapshot(second)
        assert snapshot.task.conversation_id == cid
        assert await projections.fetch_task_ids_for_conversation(db, cid) == [
            first,
            second,
        ]
        with pytest.raises(ConversationNotFound):
            await service.create_pending_task("x", conversation_id="missing")
    finally:
        await db.close()


async def test_manual_target_task_links_conversation(tmp_path, echo_agent):
    db, service = await make_service(tmp_path, echo_agent)
    try:
        created = await service.create_task("你好", TargetSpec(agent_name="echo"))
        assert created.conversation_id
        snapshot = await service.get_snapshot(created.task_id)
        assert snapshot.task.conversation_id == created.conversation_id
    finally:
        await db.close()


@pytest.fixture
async def api(tmp_path, echo_agent):
    settings = Settings(store={"db_path": tmp_path / "conv.db"})
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client, app, echo_agent.url


async def test_conversations_api(api):
    client, _, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})

    resp = await client.post(
        "/v1/tasks", json={"request": "你好", "target": {"agent_name": "echo"}}
    )
    assert resp.status_code == 201
    first = resp.json()
    cid = first["conversation_id"]
    assert cid

    resp = await client.get("/v1/conversations")
    assert resp.status_code == 200
    summaries = resp.json()
    assert len(summaries) == 1
    assert summaries[0]["id"] == cid
    assert summaries[0]["title"] == "你好"
    assert summaries[0]["task_count"] == 1
    assert summaries[0]["last_status"] == "running"

    resp = await client.get(f"/v1/conversations/{cid}")
    assert resp.status_code == 200
    detail = resp.json()
    assert detail["conversation"]["id"] == cid
    assert len(detail["tasks"]) == 1
    assert detail["tasks"][0]["task"]["conversation_id"] == cid

    resp = await client.post(
        "/v1/tasks",
        json={
            "request": "追问",
            "conversation_id": cid,
            "target": {"agent_name": "echo"},
        },
    )
    assert resp.status_code == 201
    assert resp.json()["conversation_id"] == cid

    resp = await client.get("/v1/conversations")
    assert resp.json()[0]["task_count"] == 2

    resp = await client.get("/v1/conversations/missing")
    assert resp.status_code == 404

    resp = await client.post(
        "/v1/tasks", json={"request": "x", "conversation_id": "missing"}
    )
    assert resp.status_code == 404


async def test_checkpoints_api(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    resp = await client.post(
        "/v1/tasks", json={"request": "hi", "target": {"agent_name": "echo"}}
    )
    created = resp.json()

    resp = await client.get(f"/v1/tasks/{created['task_id']}/checkpoints")
    assert resp.status_code == 200
    assert resp.json() == []

    await client.post(
        f"/v1/tasks/{created['task_id']}/nodes/{created['node_ids'][0]}/dispatch"
    )
    await app.state.task_service.create_checkpoint(created["task_id"])
    resp = await client.get(f"/v1/tasks/{created['task_id']}/checkpoints")
    assert resp.status_code == 200
    checkpoints = resp.json()
    assert len(checkpoints) == 1
    assert checkpoints[0]["plan_version"] == 1

    resp = await client.get("/v1/tasks/missing/checkpoints")
    assert resp.status_code == 404
