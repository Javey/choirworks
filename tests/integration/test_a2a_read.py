import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, create_client
from a2a.types import CancelTaskRequest, GetTaskRequest, TaskState
from a2a.utils.errors import TaskNotCancelableError, TaskNotFoundError

from choirworks.api.app import create_app
from choirworks.config import Settings


@pytest.fixture
async def hub(tmp_path, ask_agent):
    settings = Settings(
        store={"db_path": tmp_path / "read.db"},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as http:
            card = await A2ACardResolver(
                httpx_client=http, base_url="http://test"
            ).get_agent_card()
            client = await create_client(
                agent=card,
                client_config=ClientConfig(streaming=False, httpx_client=http),
            )
            yield app, http, client, ask_agent.url
            await client.close()


async def test_get_task_matches_rest_snapshot(hub):
    app, http, client, agent_url = hub
    await http.post("/v1/agents", json={"name": "ask", "card_url": agent_url})
    created = (
        await http.post(
            "/v1/tasks",
            json={"request": "hi", "target": {"agent_name": "ask", "name": "ask"}},
        )
    ).json()
    mapped = await client.get_task(GetTaskRequest(id=created["task_id"]))
    rest = (await http.get(f"/v1/tasks/{created['task_id']}")).json()
    assert mapped.id == created["task_id"]
    assert mapped.context_id == rest["task"]["conversation_id"]
    assert mapped.metadata.fields["plan_version"].number_value == 1


async def test_get_task_unknown_raises(hub):
    _, _, client, _ = hub
    with pytest.raises(TaskNotFoundError):
        await client.get_task(GetTaskRequest(id="missing"))


async def test_cancel_task_marks_canceled(hub):
    app, http, client, agent_url = hub
    await http.post("/v1/agents", json={"name": "ask", "card_url": agent_url})
    created = (
        await http.post(
            "/v1/tasks",
            json={"request": "hi", "target": {"agent_name": "ask", "name": "ask"}},
        )
    ).json()
    canceled = await client.cancel_task(CancelTaskRequest(id=created["task_id"]))
    assert canceled.status.state == TaskState.TASK_STATE_CANCELED


async def test_cancel_terminal_task_raises(hub):
    app, http, client, agent_url = hub
    await http.post("/v1/agents", json={"name": "ask", "card_url": agent_url})
    created = (
        await http.post(
            "/v1/tasks",
            json={"request": "hi", "target": {"agent_name": "ask", "name": "ask"}},
        )
    ).json()
    await client.cancel_task(CancelTaskRequest(id=created["task_id"]))
    with pytest.raises(TaskNotCancelableError):
        await client.cancel_task(CancelTaskRequest(id=created["task_id"]))
