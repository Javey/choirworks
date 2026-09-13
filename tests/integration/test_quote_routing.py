import asyncio

import httpx

from agent_hub.api.app import create_app
from agent_hub.config import Settings


async def _wait_until(check, timeout_seconds: float = 10.0):
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while asyncio.get_event_loop().time() < deadline:
        value = await check()
        if value:
            return value
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


async def make_conversation(app) -> str:
    task_id = await app.state.task_service.create_pending_task("群聊测试")
    snapshot = await app.state.task_service.get_snapshot(task_id)
    assert snapshot.task.conversation_id
    return snapshot.task.conversation_id


def make_app(tmp_path, name: str):
    settings = Settings(
        store={"db_path": tmp_path / f"{name}.db"},
        scheduler={"retry_backoff_seconds": 0.0},
        policies={"default": "human", "timeout_seconds": 30},
    )
    return create_app(settings)


async def test_quote_working_node_is_queued(tmp_path):
    from agent_hub.sim.fake_agent import start_fake_agent

    agent = await start_fake_agent("slow")
    app = make_app(tmp_path, "queued")
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            await client.post("/v1/agents", json={"name": "slow", "card_url": agent.url})
            conversation_id = await make_conversation(app)
            resp = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={"text": "hi", "mentions": ["slow"]},
            )
            task_id = resp.json()["task_id"]

            async def dispatched():
                timeline = (
                    await client.get(
                        f"/v1/conversations/{conversation_id}/messages"
                    )
                ).json()
                return next(
                    (
                        message
                        for message in timeline["messages"]
                        if message["node_id"]
                        and message["role"] == "assistant"
                        and message["text"].startswith("已派发")
                    ),
                    None,
                )

            announcement = await _wait_until(dispatched)
            quoted = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={
                    "text": "补充信息",
                    "quote_id": announcement["id"],
                },
            )
            assert quoted.status_code == 201
            assert quoted.json()["task_id"] == task_id

            timeline = (
                await client.get(f"/v1/conversations/{conversation_id}/messages")
            ).json()
            queued = [
                message
                for message in timeline["messages"]
                if message["queued_for_node_id"] and message["text"] == "补充信息"
            ]
            assert len(queued) == 1
            assert any("已排队" in message["text"] for message in timeline["messages"])
    await agent.stop()


async def test_quote_completed_agent_starts_follow_up(tmp_path, echo_agent):
    app = make_app(tmp_path, "followup")
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            await client.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
            conversation_id = await make_conversation(app)
            resp = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={"text": "hi", "mentions": ["echo"]},
            )
            first_task = resp.json()["task_id"]

            async def agent_message():
                timeline = (
                    await client.get(
                        f"/v1/conversations/{conversation_id}/messages"
                    )
                ).json()
                return next(
                    (
                        message
                        for message in timeline["messages"]
                        if message["role"] == "agent"
                    ),
                    None,
                )

            quoted = await _wait_until(agent_message)
            follow = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={"text": "再来一次", "quote_id": quoted["id"]},
            )
            assert follow.status_code == 201
            follow_task = follow.json()["task_id"]
            assert follow_task != first_task
            for _ in range(200):
                snapshot = (await client.get(f"/v1/tasks/{follow_task}")).json()
                if snapshot["task"]["status"] == "completed":
                    break
                await asyncio.sleep(0.05)
            assert snapshot["task"]["status"] == "completed"

            timeline = (
                await client.get(f"/v1/conversations/{conversation_id}/messages")
            ).json()
            user = next(
                message
                for message in timeline["messages"]
                if message["role"] == "user" and message["text"] == "再来一次"
            )
            assert user["quote_id"] == quoted["id"]
            assert user["task_id"] == follow_task


async def test_quote_intervention_answers_in_room(tmp_path):
    from agent_hub.sim.fake_agent import start_fake_agent

    agent = await start_fake_agent("ask")
    app = make_app(tmp_path, "answer")
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            await client.post("/v1/agents", json={"name": "ask", "card_url": agent.url})
            conversation_id = await make_conversation(app)
            resp = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={"text": "hi", "mentions": ["ask"]},
            )
            task_id = resp.json()["task_id"]

            async def question():
                timeline = (
                    await client.get(
                        f"/v1/conversations/{conversation_id}/messages"
                    )
                ).json()
                return next(
                    (
                        message
                        for message in timeline["messages"]
                        if message["intervention_id"]
                        and message["role"] == "assistant"
                    ),
                    None,
                )

            prompt = await _wait_until(question)
            answer = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={"text": "Bob", "quote_id": prompt["id"]},
            )
            assert answer.status_code == 201
            for _ in range(200):
                snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
                if snapshot["task"]["status"] == "completed":
                    break
                await asyncio.sleep(0.05)
            assert snapshot["task"]["status"] == "completed"

            interventions = (
                await client.get(f"/v1/tasks/{task_id}/interventions")
            ).json()
            assert interventions[0]["status"] == "resolved"
            assert interventions[0]["answer"]["text"] == "Bob"

            timeline = (
                await client.get(f"/v1/conversations/{conversation_id}/messages")
            ).json()
            user = next(
                message
                for message in timeline["messages"]
                if message["role"] == "user" and message["text"] == "Bob"
            )
            assert user["intervention_id"] == prompt["intervention_id"]
    await agent.stop()


async def test_interrupt_cancels_task_and_starts_follow_up(tmp_path):
    from agent_hub.sim.fake_agent import start_fake_agent

    agent = await start_fake_agent("slow")
    app = make_app(tmp_path, "interrupt")
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            await client.post("/v1/agents", json={"name": "slow", "card_url": agent.url})
            conversation_id = await make_conversation(app)
            resp = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={"text": "hi", "mentions": ["slow"]},
            )
            task_id = resp.json()["task_id"]

            async def dispatched():
                timeline = (
                    await client.get(
                        f"/v1/conversations/{conversation_id}/messages"
                    )
                ).json()
                return next(
                    (
                        message
                        for message in timeline["messages"]
                        if message["node_id"]
                        and message["role"] == "assistant"
                        and message["text"].startswith("已派发")
                    ),
                    None,
                )

            announcement = await _wait_until(dispatched)
            interrupted = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={
                    "text": "不要继续了",
                    "quote_id": announcement["id"],
                    "interrupt": True,
                },
            )
            assert interrupted.status_code == 201
            assert interrupted.json()["task_id"] != task_id

            async def canceled():
                snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
                return snapshot["task"]["status"] == "canceled"

            await _wait_until(canceled)
    await agent.stop()
