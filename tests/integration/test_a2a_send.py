import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import (
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageRequest,
    TaskState,
)
from a2a.utils.errors import TaskNotFoundError

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
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


def _message(
    text: str, *, task_id: str | None = None, context_id: str | None = None
) -> SendMessageRequest:
    return SendMessageRequest(
        message=Message(
            message_id="m-1",
            role=Role.ROLE_USER,
            parts=[Part(text=text)],
            task_id=task_id or "",
            context_id=context_id or "",
        )
    )


@asynccontextmanager
async def _hub(tmp_path, db_name, agent_name, agent_url, plans):
    settings = Settings(
        store={"db_path": tmp_path / db_name},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
        policies={"default": "human", "timeout_seconds": 30},
    )
    app = create_app(settings, llm=FakeLLM(structured_results=list(plans)))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as http:
            await http.post(
                "/v1/agents", json={"name": agent_name, "card_url": agent_url}
            )
            card = await A2ACardResolver(
                httpx_client=http, base_url="http://test"
            ).get_agent_card()
            client = await create_client(
                agent=card,
                client_config=ClientConfig(streaming=False, httpx_client=http),
            )
            yield app, http, client
            await client.close()


@pytest.fixture
async def hub_echo(tmp_path, echo_agent):
    async with _hub(
        tmp_path, "echo.db", "echo", echo_agent.url, [_plan("echo")] * 4
    ) as value:
        yield value


@pytest.fixture
async def hub_ask(tmp_path, ask_agent):
    async with _hub(
        tmp_path, "ask.db", "ask", ask_agent.url, [_plan("ask")] * 2
    ) as value:
        yield value


async def _send(client, request: SendMessageRequest):
    responses = [response async for response in client.send_message(request)]
    assert responses
    last = responses[-1]
    if last.WhichOneof("payload") == "task":
        return last.task
    return last.message


async def test_send_creates_task_and_conversation(hub_echo):
    _, http, client = hub_echo
    task = await _send(client, _message("请评估这个问题"))
    assert task.id
    assert task.context_id
    timeline = (
        await http.get(f"/v1/conversations/{task.context_id}/messages")
    ).json()
    assert timeline["messages"][0]["role"] == "user"


async def test_send_with_context_reuses_conversation(hub_echo):
    _, _, client = hub_echo
    first = await _send(client, _message("第一个任务"))
    second = await _send(client, _message("第二个任务", context_id=first.context_id))
    assert second.context_id == first.context_id
    assert second.id != first.id


async def test_send_terminal_task_creates_followup(hub_echo):
    app, http, client = hub_echo
    first = await _send(client, _message("第一个任务"))
    for _ in range(200):
        status = (await http.get(f"/v1/tasks/{first.id}")).json()["task"]["status"]
        if status in {"completed", "failed", "canceled"}:
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("first task never reached terminal state")
    second = await _send(client, _message("继续", task_id=first.id))
    assert second.id != first.id
    assert second.context_id == first.context_id


async def test_send_answers_pending_intervention(hub_ask):
    app, http, client = hub_ask
    first = await _send(client, _message("请评估"))
    for _ in range(200):
        task = await client.get_task(GetTaskRequest(id=first.id))
        if task.status.state == TaskState.TASK_STATE_INPUT_REQUIRED:
            break
        await asyncio.sleep(0.05)
    else:
        raise AssertionError("task never reached input-required")
    resumed = await _send(client, _message("这是答复", task_id=first.id))
    assert resumed.id == first.id


async def test_send_running_task_stays_same(tmp_path):
    slow = await start_fake_agent("slow")
    try:
        async with _hub(tmp_path, "slow.db", "slow", slow.url, []) as (
            _app,
            _http,
            client,
        ):
            first = await _send(client, _message("@slow 开始"))
            second = await _send(client, _message("补充说明", task_id=first.id))
            assert second.id == first.id
    finally:
        await slow.stop()


async def test_send_unknown_task_raises(hub_echo):
    _, _, client = hub_echo
    with pytest.raises(TaskNotFoundError):
        await _send(client, _message("继续", task_id="missing"))
