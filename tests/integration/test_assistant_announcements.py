import asyncio

import httpx
import pytest

from agent_hub.api.app import create_app
from agent_hub.config import Settings
from agent_hub.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM


@pytest.fixture
async def api(tmp_path, echo_agent):
    plan = PlanDraft(
        rationale="room",
        nodes=[
            PlanNodeDraft(id="n1", name="echo", agent_name="echo", input={"text": "hi"})
        ],
    )
    settings = Settings(
        store={"db_path": tmp_path / "announce.db"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    app = create_app(settings, llm=FakeLLM(structured_results=[plan]))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield client, app, echo_agent.url


async def wait_status(client, task_id: str, status: str) -> dict:
    snapshot = None
    for _ in range(200):
        snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
        if snapshot["task"]["status"] == status:
            return snapshot
        await asyncio.sleep(0.05)
    raise AssertionError(f"task not {status}: {snapshot}")


async def make_conversation(app) -> str:
    task_id = await app.state.task_service.create_pending_task("群聊测试")
    snapshot = await app.state.task_service.get_snapshot(task_id)
    assert snapshot.task.conversation_id
    return snapshot.task.conversation_id


async def test_assistant_announces_plan_dispatch_and_completion(api):
    client, app, agent_url = api
    await client.post("/v1/agents", json={"name": "echo", "card_url": agent_url})
    conversation_id = await make_conversation(app)

    resp = await client.post(
        f"/v1/conversations/{conversation_id}/messages", json={"text": "hi"}
    )
    await wait_status(client, resp.json()["task_id"], "completed")

    timeline = (await client.get(f"/v1/conversations/{conversation_id}/messages")).json()
    texts = [message["text"] for message in timeline["messages"]]
    assert any(text.startswith("任务已拆解") for text in texts)
    assert any(text.startswith("已派发 @echo") for text in texts)
    assert any(text.startswith("任务完成") for text in texts)
    assert [message["role"] for message in timeline["messages"]].count("agent") == 1


async def test_human_intervention_question_and_answer_in_room(tmp_path, ask_agent):
    plan = PlanDraft(
        rationale="ask",
        nodes=[
            PlanNodeDraft(id="n1", name="n1", agent_name="echo", input={"text": "ask"})
        ],
    )
    settings = Settings(
        store={"db_path": tmp_path / "ask.db"},
        scheduler={"retry_backoff_seconds": 0.0},
        policies={"default": "human", "timeout_seconds": 30},
    )
    app = create_app(settings, llm=FakeLLM(structured_results=[plan]))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            await client.post("/v1/agents", json={"name": "echo", "card_url": ask_agent.url})
            conversation_id = await make_conversation(app)
            resp = await client.post(
                f"/v1/conversations/{conversation_id}/messages", json={"text": "hi"}
            )
            task_id = resp.json()["task_id"]
            await wait_status(client, task_id, "awaiting_input")

            interventions = (
                await client.get(f"/v1/tasks/{task_id}/interventions?status=pending")
            ).json()
            assert len(interventions) == 1
            intervention_id = interventions[0]["id"]

            timeline = (
                await client.get(f"/v1/conversations/{conversation_id}/messages")
            ).json()
            question = next(
                message
                for message in timeline["messages"]
                if message["intervention_id"] == intervention_id
            )
            assert question["role"] == "assistant"
            assert "需要确认" in question["text"]

            answer = await client.post(
                f"/v1/tasks/{task_id}/interventions/{intervention_id}",
                json={"text": "Bob", "responder": "user"},
            )
            assert answer.status_code == 200
            await wait_status(client, task_id, "completed")

            timeline = (
                await client.get(f"/v1/conversations/{conversation_id}/messages")
            ).json()
            user_answers = [
                message
                for message in timeline["messages"]
                if message["role"] == "user"
                and message["intervention_id"] == intervention_id
            ]
            assert [message["text"] for message in user_answers] == ["Bob"]
