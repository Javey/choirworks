import asyncio

import httpx

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.sim.fake_agent import start_fake_agent
from choirworks.sim.llm import SimLLM
from tests.support.fakes import FakeLLM


def index_of(texts: list[str], needle: str) -> int:
    for index, text in enumerate(texts):
        if needle in text:
            return index
    raise AssertionError(f"not found: {needle}\n{texts}")


async def make_conversation(client) -> str:
    created = await client.post("/v1/conversations", json={"title": "叙事顺序"})
    return created.json()["conversation_id"]


async def wait_completed(client, task_id: str, timeout_seconds: float = 15.0):
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    snapshot = None
    while asyncio.get_event_loop().time() < deadline:
        snapshot = (await client.get(f"/v1/tasks/{task_id}")).json()
        if snapshot["task"]["status"] in ("completed", "failed"):
            return snapshot
        await asyncio.sleep(0.05)
    raise AssertionError(f"task not terminal: {snapshot}")


async def test_planner_narrative_order(tmp_path, echo_agent):
    plan = PlanDraft(
        rationale="narrative",
        nodes=[
            PlanNodeDraft(id="n1", name="echo", agent_name="echo", input={"text": "调研"}),
            PlanNodeDraft(id="n2", name="writer", agent_name="writer", input={"text": "写作"}),
        ],
    )
    settings = Settings(
        store={"db_path": tmp_path / "order-plan.db"},
        scheduler={"retry_backoff_seconds": 0.0, "max_parallel_nodes": 1},
    )
    app = create_app(settings, llm=FakeLLM(structured_results=[plan]))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            await client.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
            await client.post("/v1/agents", json={"name": "writer", "card_url": echo_agent.url})
            conversation_id = await make_conversation(client)
            resp = await client.post(
                f"/v1/conversations/{conversation_id}/messages",
                json={"text": "帮我调研并写报告"},
            )
            await wait_completed(client, resp.json()["task_id"])

            timeline = (
                await client.get(f"/v1/conversations/{conversation_id}/messages")
            ).json()
            texts = [message["text"] for message in timeline["messages"]]

            user = index_of(texts, "帮我调研并写报告")
            plan_index = index_of(texts, "任务已拆解")
            join_echo = index_of(texts, "已将 @echo 加入群聊")
            join_writer = index_of(texts, "已将 @writer 加入群聊")
            dispatch_echo = index_of(texts, "已派发 @echo")
            dispatch_writer = index_of(texts, "已派发 @writer")
            assert user < plan_index < join_echo < join_writer
            assert join_echo < dispatch_echo
            assert join_writer < dispatch_writer


async def test_peer_assist_narrative_order(tmp_path):
    researcher = await start_fake_agent("collaborate", name="researcher")
    analyst = await start_fake_agent("assist", name="analyst")
    try:
        settings = Settings(
            store={"db_path": tmp_path / "order-peer.db"},
            policies={"overrides": [{"agent_name": "researcher", "policy": "peer_agent"}]},
            scheduler={"retry_backoff_seconds": 0.0},
        )
        app = create_app(settings, llm=SimLLM())
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                await client.post(
                    "/v1/agents", json={"name": "researcher", "card_url": researcher.url}
                )
                await client.post(
                    "/v1/agents", json={"name": "analyst", "card_url": analyst.url}
                )
                conversation_id = await make_conversation(client)
                resp = await client.post(
                    f"/v1/conversations/{conversation_id}/messages",
                    json={
                        "text": "请协调多个子代理协作完成这项分析",
                        "mentions": ["researcher"],
                    },
                )
                await wait_completed(client, resp.json()["task_id"])

                timeline = (
                    await client.get(f"/v1/conversations/{conversation_id}/messages")
                ).json()
                texts = [message["text"] for message in timeline["messages"]]

                request = index_of(texts, "需要 C 参与确认技术细节")
                decision = index_of(texts, "请求 @analyst 协助")
                joined = index_of(texts, "已将 @analyst 加入群聊")
                dispatched = index_of(texts, "已派发 @analyst")
                assert request < decision < joined < dispatched
    finally:
        await researcher.stop()
        await analyst.stop()
