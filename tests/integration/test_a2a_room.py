import asyncio

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import GetTaskRequest, TaskState

from choirworks.a2a.mapping import A2A_ROOM_URI
from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.core.room import post_message
from choirworks.sim.fake_agent import start_fake_agent
from tests.support.fakes import FakeLLM


def _plan(agent_name: str) -> PlanDraft:
    return PlanDraft(
        rationale="single",
        nodes=[
            PlanNodeDraft(
                id="n1",
                name=agent_name,
                agent_name=agent_name,
                input={"text": "问题"},
            )
        ],
    )


def _settings(tmp_path, db_name: str, *, port: int | None = None) -> Settings:
    public = f"http://127.0.0.1:{port}" if port is not None else "http://test"
    return Settings(
        store={"db_path": tmp_path / db_name},
        a2a={"public_url": public},
        scheduler={"retry_backoff_seconds": 0.0},
    )


async def _connect(app):
    transport = httpx.ASGITransport(app=app)
    http = httpx.AsyncClient(transport=transport, base_url="http://test")
    card = await A2ACardResolver(
        httpx_client=http, base_url="http://test"
    ).get_agent_card()
    client = await create_client(
        agent=card,
        client_config=ClientConfig(streaming=False, httpx_client=http),
    )
    return http, client


async def _new_room(http, title: str = "测试群") -> str:
    created = (await http.post("/v1/conversations", json={"title": title})).json()
    return created["conversation_id"]


async def _post_room_message(app, conversation_id: str, text: str):
    return await post_message(
        app.state.db,
        app.state.event_store,
        conversation_id=conversation_id,
        role="user",
        sender="CEO",
        text=text,
    )


async def _wait_for_active_node(http, task_id: str) -> None:
    for _ in range(200):
        nodes = (await http.get(f"/v1/tasks/{task_id}")).json()["nodes"]
        if any(node["status"] in {"dispatched", "working"} for node in nodes):
            return
        await asyncio.sleep(0.05)
    raise AssertionError("no active node appeared")


@pytest.fixture
async def hub_room(tmp_path, echo_agent):
    app = create_app(
        _settings(tmp_path, "room.db"),
        llm=FakeLLM(structured_results=[_plan("echo")] * 4),
    )
    async with app.router.lifespan_context(app):
        http, client = await _connect(app)
        await http.post(
            "/v1/agents", json={"name": "echo", "card_url": echo_agent.url}
        )
        yield app, http, client
        await client.close()
        await http.aclose()


async def test_get_room_task_maps_history_members(hub_room):
    app, http, client = hub_room
    conversation_id = await _new_room(http)
    await _post_room_message(app, conversation_id, "大家早上好")
    await app.state.coordinator.join_new_members(conversation_id, ["echo"])

    room = await client.get_task(GetTaskRequest(id=conversation_id))

    assert room.id == conversation_id
    assert room.context_id == conversation_id
    assert room.status.state is TaskState.TASK_STATE_INPUT_REQUIRED
    assert [message.parts[0].text for message in room.history] == [
        "大家早上好",
        "已将 @echo 加入群聊",
    ]
    fields = room.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["kind"].string_value == "room"
    assert fields["title"].string_value == "测试群"
    assert fields["message_count"].number_value == 2
    member = fields["members"].list_value.values[0].struct_value.fields
    assert member["agent_name"].string_value == "echo"


async def test_get_room_task_working_while_task_runs(tmp_path):
    slow = await start_fake_agent("slow")
    try:
        app = create_app(_settings(tmp_path, "room_working.db"))
        async with app.router.lifespan_context(app):
            http, client = await _connect(app)
            try:
                await http.post(
                    "/v1/agents", json={"name": "slow", "card_url": slow.url}
                )
                conversation_id = await _new_room(http, "运行群")
                posted = (
                    await http.post(
                        f"/v1/conversations/{conversation_id}/messages",
                        json={"text": "@slow 开始", "mentions": ["slow"]},
                    )
                ).json()
                await _wait_for_active_node(http, posted["task_id"])

                room = await client.get_task(GetTaskRequest(id=conversation_id))
            finally:
                await client.close()
                await http.aclose()
    finally:
        await slow.stop()

    assert room.status.state is TaskState.TASK_STATE_WORKING
