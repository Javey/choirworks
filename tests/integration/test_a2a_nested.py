import pytest
from a2a.server.context import ServerCallContext
from a2a.types import (
    ListTasksRequest,
    Message,
    Part,
    Role,
    SendMessageRequest,
    TaskState,
)

from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM
from tests.support.hubs import start_hub
from tests.support.sdk import sdk_hub, wait_for_task

pytestmark = pytest.mark.skip(reason="execute 暂为 plan-only，不派发")


def _settings(port: int, db: str) -> Settings:
    return Settings(
        store={"db_path": db},
        a2a={"public_url": f"http://127.0.0.1:{port}"},
        scheduler={"retry_backoff_seconds": 0.0},
    )


def _plan(agent_name: str) -> PlanDraft:
    return PlanDraft(
        nodes=[
            PlanNodeDraft(
                id="n1", name=agent_name, agent_name=agent_name, input={"text": "hi"}
            )
        ],
    )


async def test_outer_dispatches_task_to_inner_instance(tmp_path, echo_agent):
    async with start_hub(
        lambda port: _settings(port, str(tmp_path / "inner.db")),
        llm=FakeLLM(structured_results=[_plan("echo")]),
    ) as (inner_app, inner_url):
        await inner_app.state.registry.register("echo", echo_agent.url)
        async with sdk_hub(
            tmp_path,
            "outer.db",
            plans=[_plan("inner")],
            settings=Settings(
                store={"db_path": tmp_path / "outer.db"},
                a2a={"public_url": "http://test"},
                scheduler={"retry_backoff_seconds": 0.0},
            ),
        ) as (_outer_app, http, client):
            resp = await http.post(
                "/v1/agents", json={"name": "inner", "card_url": inner_url}
            )
            assert resp.status_code == 201, resp.text
            task_id = ""
            async for response in client.send_message(
                SendMessageRequest(
                    message=Message(
                        message_id="m-1",
                        role=Role.ROLE_USER,
                        parts=[Part(text="请内层实例处理这个请求")],
                    )
                )
            ):
                if response.WhichOneof("payload") == "task":
                    task_id = response.task.id
            outer_task = await wait_for_task(
                client, task_id, {TaskState.TASK_STATE_COMPLETED}
            )
            artifact_text = " ".join(
                part.text
                for artifact in outer_task.artifacts
                for part in artifact.parts
                if part.HasField("text")
            )
            assert "echo:hi" in artifact_text
            inner_tasks = await inner_app.state.task_store.list(
                ListTasksRequest(), ServerCallContext()
            )
            assert len(inner_tasks.tasks) == 1
