import asyncio

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import (
    Message,
    Part,
    Role,
    SendMessageRequest,
    SubscribeToTaskRequest,
    TaskState,
)

from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.sim.fake_agent import start_fake_agent
from tests.support.fakes import FakeLLM
from tests.support.hubs import start_hub


def _settings(port: int, db: str) -> Settings:
    return Settings(
        store={"db_path": db},
        a2a={"public_url": f"http://127.0.0.1:{port}"},
        scheduler={"retry_backoff_seconds": 0.0},
    )


def _send(text: str) -> SendMessageRequest:
    return SendMessageRequest(
        message=Message(
            message_id="m-1", role=Role.ROLE_USER, parts=[Part(text=text)]
        )
    )


async def _connect(base_url: str):
    http = httpx.AsyncClient(base_url=base_url, timeout=10.0)
    card = await A2ACardResolver(httpx_client=http, base_url=base_url).get_agent_card()
    client = await create_client(
        agent=card, client_config=ClientConfig(streaming=True, httpx_client=http)
    )
    return http, client


@pytest.fixture
async def hub(tmp_path):
    plan = PlanDraft(
        rationale="single",
        nodes=[
            PlanNodeDraft(
                id="n1", name="echo", agent_name="echo", input={"text": "hi"}
            )
        ],
    )
    agent = await start_fake_agent("delay")
    try:
        async with start_hub(
            lambda port: _settings(port, str(tmp_path / "stream.db")),
            llm=FakeLLM(structured_results=[plan]),
        ) as (app, base_url):
            await app.state.registry.register("echo", agent.url)
            yield app, base_url
    finally:
        await agent.stop()


async def test_streaming_send_emits_task_updates_and_artifacts(hub):
    _, base_url = hub
    http, client = await _connect(base_url)
    try:
        responses = [r async for r in client.send_message(_send("分析 X"))]
    finally:
        await client.close()
        await http.aclose()
    assert responses[0].WhichOneof("payload") == "task"
    kinds = [r.WhichOneof("payload") for r in responses]
    assert kinds[0] == "task"
    assert "artifact_update" in kinds
    assert "status_update" in kinds
    artifacts = [
        r.artifact_update
        for r in responses
        if r.WhichOneof("payload") == "artifact_update"
    ]
    assert any(":n1:" in a.artifact.artifact_id for a in artifacts)
    assert any(a.append for a in artifacts)
    terminal = [
        r
        for r in responses
        if r.WhichOneof("payload") == "status_update"
        and r.status_update.status.state
        in {
            TaskState.TASK_STATE_COMPLETED,
            TaskState.TASK_STATE_FAILED,
            TaskState.TASK_STATE_CANCELED,
        }
    ]
    assert terminal


async def test_subscribe_replays_snapshot_then_live(hub):
    app, base_url = hub
    http, client = await _connect(base_url)
    try:
        created = (
            await http.post(
                "/v1/tasks",
                json={"request": "hi", "target": {"agent_name": "echo", "name": "echo"}},
            )
        ).json()
        dispatch = asyncio.create_task(
            http.post(
                f"/v1/tasks/{created['task_id']}"
                f"/nodes/{created['node_ids'][0]}/dispatch"
            )
        )
        responses = [
            r async for r in client.subscribe(
                SubscribeToTaskRequest(id=created["task_id"])
            )
        ]
        await dispatch
    finally:
        await client.close()
        await http.aclose()
    assert responses[0].WhichOneof("payload") == "task"
    assert responses[0].task.id == created["task_id"]
    last = responses[-1]
    if last.WhichOneof("payload") == "status_update":
        assert last.status_update.status.state in {
            TaskState.TASK_STATE_COMPLETED,
            TaskState.TASK_STATE_FAILED,
            TaskState.TASK_STATE_CANCELED,
        }
    else:
        assert last.task.status.state in {
            TaskState.TASK_STATE_COMPLETED,
            TaskState.TASK_STATE_FAILED,
            TaskState.TASK_STATE_CANCELED,
        }


async def test_subscribe_completed_task_returns_snapshot_only(hub):
    app, base_url = hub
    http, client = await _connect(base_url)
    try:
        created = (
            await http.post(
                "/v1/tasks",
                json={"request": "hi", "target": {"agent_name": "echo", "name": "echo"}},
            )
        ).json()
        await http.post(
            f"/v1/tasks/{created['task_id']}"
            f"/nodes/{created['node_ids'][0]}/dispatch"
        )
        for _ in range(200):
            status = (
                await http.get(f"/v1/tasks/{created['task_id']}")
            ).json()["task"]["status"]
            if status == "completed":
                break
            await asyncio.sleep(0.05)
        responses = [
            r async for r in client.subscribe(
                SubscribeToTaskRequest(id=created["task_id"])
            )
        ]
    finally:
        await client.close()
        await http.aclose()
    assert len(responses) == 1
    assert responses[0].WhichOneof("payload") == "task"
    assert responses[0].task.status.state is TaskState.TASK_STATE_COMPLETED
