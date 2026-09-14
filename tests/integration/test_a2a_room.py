import asyncio

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import (
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageRequest,
    SubscribeToTaskRequest,
    TaskState,
)
from a2a.utils.errors import InvalidParamsError, TaskNotFoundError

from choirworks.a2a.mapping import A2A_ROOM_URI, struct_value
from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.core.room import post_message
from choirworks.sim.fake_agent import start_fake_agent
from tests.support.fakes import FakeLLM
from tests.support.hubs import start_hub


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


async def _connect(app, *, streaming: bool = False):
    transport = httpx.ASGITransport(app=app)
    http = httpx.AsyncClient(transport=transport, base_url="http://test")
    card = await A2ACardResolver(
        httpx_client=http, base_url="http://test"
    ).get_agent_card()
    client = await create_client(
        agent=card,
        client_config=ClientConfig(streaming=streaming, httpx_client=http),
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


def _send(
    text: str,
    *,
    context_id: str | None = None,
    room_meta: dict | None = None,
) -> SendMessageRequest:
    message = Message(
        message_id="m-1",
        role=Role.ROLE_USER,
        parts=[Part(text=text)],
        context_id=context_id or "",
    )
    if room_meta is not None:
        message.metadata.CopyFrom(struct_value({A2A_ROOM_URI: room_meta}))
    return SendMessageRequest(message=message)


async def test_send_with_context_creates_task_in_room(hub_room):
    app, http, client = hub_room
    conversation_id = await _new_room(http)

    responses = [
        response
        async for response in client.send_message(
            _send("@echo 请处理", context_id=conversation_id)
        )
    ]

    task = responses[-1].task
    assert task.id != conversation_id
    assert task.context_id == conversation_id
    timeline = (
        await http.get(f"/v1/conversations/{conversation_id}/messages")
    ).json()["messages"]
    assert timeline[0]["text"] == "@echo 请处理"


async def test_send_queued_returns_message(tmp_path):
    slow = await start_fake_agent("slow")
    try:
        app = create_app(_settings(tmp_path, "room_queue.db"))
        async with app.router.lifespan_context(app):
            http, client = await _connect(app)
            try:
                await http.post(
                    "/v1/agents", json={"name": "slow", "card_url": slow.url}
                )
                conversation_id = await _new_room(http, "排队群")
                posted = (
                    await http.post(
                        f"/v1/conversations/{conversation_id}/messages",
                        json={"text": "@slow 开始", "mentions": ["slow"]},
                    )
                ).json()
                await _wait_for_active_node(http, posted["task_id"])
                quote_id = None
                for _ in range(200):
                    messages = (
                        await http.get(
                            f"/v1/conversations/{conversation_id}/messages"
                        )
                    ).json()["messages"]
                    announcements = [
                        message
                        for message in messages
                        if message["role"] == "assistant" and message["node_id"]
                    ]
                    if announcements:
                        quote_id = announcements[-1]["id"]
                        break
                    await asyncio.sleep(0.05)
                else:
                    raise AssertionError("no dispatch announcement")

                responses = [
                    response
                    async for response in client.send_message(
                        _send(
                            "补充一句",
                            context_id=conversation_id,
                            room_meta={"quote_id": quote_id},
                        )
                    )
                ]
            finally:
                await client.close()
                await http.aclose()
    finally:
        await slow.stop()

    assert responses[-1].WhichOneof("payload") == "message"
    reply = responses[-1].message
    assert reply.parts[0].text == "补充一句"
    fields = reply.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["kind"].string_value == "message"
    assert fields["quote_id"].string_value == quote_id
    assert fields["queued_for_node_id"].string_value


async def test_send_interrupt_requires_quote(hub_room):
    _, _, client = hub_room
    with pytest.raises(InvalidParamsError):
        async for _ in client.send_message(
            _send("打断", room_meta={"interrupt": True})
        ):
            pass


async def test_subscribe_room_streams_live_messages(tmp_path):
    async with start_hub(
        lambda port: _settings(tmp_path, "room_stream.db", port=port)
    ) as (app, base_url):
        http = httpx.AsyncClient(base_url=base_url, timeout=10.0)
        card = await A2ACardResolver(
            httpx_client=http, base_url=base_url
        ).get_agent_card()
        client = await create_client(
            agent=card,
            client_config=ClientConfig(streaming=True, httpx_client=http),
        )
        try:
            created = (
                await http.post("/v1/conversations", json={"title": "直播群"})
            ).json()
            conversation_id = created["conversation_id"]
            stream = client.subscribe(SubscribeToTaskRequest(id=conversation_id))
            first = await asyncio.wait_for(anext(stream), timeout=10)
            assert first.WhichOneof("payload") == "task"
            assert first.task.id == conversation_id
            assert first.task.status.state is TaskState.TASK_STATE_INPUT_REQUIRED

            posting = asyncio.create_task(
                _post_room_message(app, conversation_id, "直播消息")
            )
            frame = None
            for _ in range(20):
                candidate = await asyncio.wait_for(anext(stream), timeout=10)
                if candidate.WhichOneof("payload") == "message":
                    frame = candidate
                    break
            await posting
            await stream.aclose()
        finally:
            await client.close()
            await http.aclose()

    assert frame is not None
    assert frame.message.parts[0].text == "直播消息"
    fields = frame.message.metadata.fields[A2A_ROOM_URI].struct_value.fields
    assert fields["kind"].string_value == "message"
    assert fields["seq"].number_value == 1


async def test_subscribe_unknown_raises(hub_room):
    app, _, _ = hub_room
    http, stream_client = await _connect(app, streaming=True)
    try:
        with pytest.raises(TaskNotFoundError):
            async for _ in stream_client.subscribe(
                SubscribeToTaskRequest(id="missing")
            ):
                pass
    finally:
        await stream_client.close()
        await http.aclose()


async def test_room_task_is_readable_without_extension_activation(hub_room):
    app, http, client = hub_room
    conversation_id = await _new_room(http)
    await _post_room_message(app, conversation_id, "纯文本消息")

    room = await client.get_task(GetTaskRequest(id=conversation_id))

    assert room.history
    for message in room.history:
        assert message.message_id
        assert message.role in {Role.ROLE_USER, Role.ROLE_AGENT}
        assert message.parts[0].text
