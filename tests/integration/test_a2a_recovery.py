import asyncio

from a2a.server.context import ServerCallContext
from a2a.types import Message, Part, Role, SendMessageRequest, TaskState

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.sim.fake_agent import start_fake_agent
from tests.support.fakes import FakeLLM
from tests.support.sdk import task_nodes


def _settings(db_path) -> Settings:
    return Settings(
        store={"db_path": db_path},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
    )


def _plan(agent_name: str) -> PlanDraft:
    return PlanDraft(
        nodes=[
            PlanNodeDraft(
                id="n1", name=agent_name, agent_name=agent_name, input={"text": "开始"}
            )
        ],
    )


async def test_recover_inflight_task_after_restart(tmp_path):
    slow = await start_fake_agent("slow")
    db_path = tmp_path / "recovery.db"
    try:
        app = await create_app(
            _settings(db_path),
            llm=FakeLLM(structured_results=[_plan("slow")] * 2),
        )
        async with app.router.lifespan_context(app):
            await app.state.registry.register("slow", slow.url)
            request_handler = app.state.request_handler
            task = await request_handler.on_message_send(
                SendMessageRequest(
                    message=Message(
                        message_id="m-1",
                        role=Role.ROLE_USER,
                        parts=[Part(text="开始")],
                    )
                ),
                ServerCallContext(),
            )
            task_id = task.id
            for _ in range(200):
                snapshot = await app.state.task_store.get(task_id, ServerCallContext())
                if task_nodes(snapshot)["n1"]["status"] in {"dispatched", "working"}:
                    break
                await asyncio.sleep(0.05)

        # "Restart": new app on the same DB, lifespan recovery re-attaches work.
        app2 = await create_app(_settings(db_path), llm=FakeLLM())
        async with app2.router.lifespan_context(app2):
            for _ in range(300):
                snapshot = await app2.state.task_store.get(
                    task_id, ServerCallContext()
                )
                if snapshot.status.state in {
                    TaskState.TASK_STATE_COMPLETED,
                    TaskState.TASK_STATE_FAILED,
                }:
                    break
                await asyncio.sleep(0.05)
            assert snapshot.status.state == TaskState.TASK_STATE_COMPLETED
            assert task_nodes(snapshot)["n1"]["status"] == "completed"
    finally:
        await slow.stop()
