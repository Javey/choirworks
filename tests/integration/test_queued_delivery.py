import asyncio

import httpx

from agent_hub.api.app import create_app
from agent_hub.config import Settings
from agent_hub.core.room import post_message
from agent_hub.store import projections


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


async def test_queued_message_delivered_on_continue(tmp_path):
    from agent_hub.sim.fake_agent import start_fake_agent

    agent = await start_fake_agent("ask")
    app = make_app(tmp_path, "deliver")
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
                        if message["node_id"]
                        and message["role"] == "assistant"
                        and message["text"].startswith("已派发")
                    ),
                    None,
                )

            announcement = await _wait_until(question)
            queued = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={"text": "补充：用中文", "quote_id": announcement["id"]},
            )
            assert queued.status_code == 201

            interventions = (
                await client.get(f"/v1/tasks/{task_id}/interventions?status=pending")
            ).json()
            answer = await client.post(
                f"/v1/tasks/{task_id}/interventions/{interventions[0]['id']}",
                json={"text": "Bob", "responder": "user"},
            )
            assert answer.status_code == 200

            async def completed():
                snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
                return snapshot["task"]["status"] == "completed"

            await _wait_until(completed)

            timeline = (
                await client.get(f"/v1/conversations/{conversation_id}/messages")
            ).json()
            supplement = next(
                message
                for message in timeline["messages"]
                if message["text"] == "补充：用中文"
            )
            assert supplement["delivered_at"] is not None
    await agent.stop()


async def test_reconcile_forwards_queued_message_for_terminal_node(tmp_path, echo_agent):
    app = make_app(tmp_path, "reconcile")
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            await client.post(
                "/v1/agents", json={"name": "echo", "card_url": echo_agent.url}
            )
            conversation_id = await make_conversation(app)
            resp = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={"text": "hi", "mentions": ["echo"]},
            )
            original_task = resp.json()["task_id"]

            async def completed():
                snapshot = (await client.get(f"/v1/tasks/{original_task}")).json()
                return snapshot["task"]["status"] == "completed"

            await _wait_until(completed)
            snapshot = (
                await client.get(f"/v1/tasks/{original_task}")
            ).json()
            node_id = snapshot["nodes"][0]["id"]

            db = app.state.db
            events = app.state.event_store
            message = await post_message(
                db,
                events,
                conversation_id=conversation_id,
                role="user",
                sender="CEO",
                text="事后补充",
                task_id=original_task,
                node_id=node_id,
                queued_for_node_id=node_id,
            )
            forwarded = await app.state.coordinator.reconcile()
            assert forwarded == 1
            refreshed = await projections.fetch_message(db, message.id)
            assert refreshed is not None and refreshed.delivered_at is not None

            timeline = (
                await client.get(f"/v1/conversations/{conversation_id}/messages")
            ).json()
            assert any("已转交" in item["text"] for item in timeline["messages"])
