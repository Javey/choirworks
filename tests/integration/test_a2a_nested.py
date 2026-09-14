import asyncio

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import Message, Part, Role, SendMessageRequest

from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM
from tests.support.hubs import start_hub


def _settings(port: int, db: str) -> Settings:
    return Settings(
        store={"db_path": db},
        a2a={"public_url": f"http://127.0.0.1:{port}"},
        scheduler={"retry_backoff_seconds": 0.0},
    )


def _inner_plan() -> PlanDraft:
    return PlanDraft(
        rationale="inner",
        nodes=[
            PlanNodeDraft(
                id="n1", name="echo", agent_name="echo", input={"text": "hi"}
            )
        ],
    )


async def _connect(base_url: str):
    http = httpx.AsyncClient(base_url=base_url, timeout=10.0)
    card = await A2ACardResolver(httpx_client=http, base_url=base_url).get_agent_card()
    client = await create_client(
        agent=card, client_config=ClientConfig(streaming=False, httpx_client=http)
    )
    return http, client


@pytest.fixture
async def hubs(tmp_path, echo_agent):
    async with start_hub(
        lambda port: _settings(port, str(tmp_path / "inner.db")),
        llm=FakeLLM(structured_results=[_inner_plan()]),
    ) as (inner_app, inner_url):
        await inner_app.state.registry.register("echo", echo_agent.url)
        async with start_hub(
            lambda port: _settings(port, str(tmp_path / "outer.db"))
        ) as (outer_app, outer_url):
            http, client = await _connect(outer_url)
            try:
                resp = await http.post(
                    "/v1/agents", json={"name": "inner", "card_url": inner_url}
                )
                assert resp.status_code == 201, resp.text
                yield outer_app, http, client, inner_app, inner_url
            finally:
                await client.close()
                await http.aclose()


async def test_outer_dispatches_task_to_inner_instance(hubs):
    _outer_app, http, client, inner_app, _inner_url = hubs
    responses = [
        response
        async for response in client.send_message(
            SendMessageRequest(
                message=Message(
                    message_id="m-1",
                    role=Role.ROLE_USER,
                    parts=[Part(text="@inner 请处理这个请求")],
                )
            )
        )
    ]
    assert responses[-1].WhichOneof("payload") == "task"
    outer_task = responses[-1].task
    assert outer_task.context_id

    snapshot = None
    for _ in range(400):
        snapshot = (await http.get(f"/v1/tasks/{outer_task.id}")).json()
        if snapshot["task"]["status"] in {"completed", "failed"}:
            break
        await asyncio.sleep(0.05)
    assert snapshot["task"]["status"] == "completed", snapshot
    artifacts = [
        node["output"]["artifacts"]
        for node in snapshot["nodes"]
        if node.get("output")
    ]
    assert artifacts
    assert artifacts[0][0]["text"]

    cursor = await inner_app.state.db.conn.execute(
        "SELECT COUNT(*) FROM orchestration_tasks"
    )
    assert (await cursor.fetchone())[0] == 1

    timeline = (
        await http.get(f"/v1/conversations/{outer_task.context_id}/messages")
    ).json()
    senders = {message["sender"] for message in timeline["messages"]}
    assert "inner" in senders or any(
        message["role"] == "agent" for message in timeline["messages"]
    )
