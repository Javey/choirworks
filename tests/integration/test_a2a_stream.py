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
from a2a.utils.errors import InvalidParamsError

from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.sim.fake_agent import start_fake_agent
from tests.support.sdk import sdk_hub, wait_for_task


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


def _plan(agent_name: str) -> PlanDraft:
    return PlanDraft(
        rationale="single",
        nodes=[
            PlanNodeDraft(
                id="n1", name=agent_name, agent_name=agent_name, input={"text": "hi"}
            )
        ],
    )


async def _connect(base_url: str):
    http = httpx.AsyncClient(base_url=base_url, timeout=10.0)
    card = await A2ACardResolver(httpx_client=http, base_url=base_url).get_agent_card()
    client = await create_client(
        agent=card, client_config=ClientConfig(streaming=True, httpx_client=http)
    )
    return http, client


async def test_streaming_send_emits_plan(tmp_path, echo_agent):
    async with sdk_hub(
        tmp_path, "stream.db", plans=[_plan("echo")] * 2, streaming=True
    ) as (_app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        responses = [r async for r in client.send_message(_send("分析 X"))]
        assert responses[0].WhichOneof("payload") == "task"
        kinds = [
            r.status_update.metadata.fields["kind"].string_value
            for r in responses
            if r.WhichOneof("payload") == "status_update"
            and "kind" in r.status_update.metadata.fields
        ]
        assert "plan.created" in kinds
        thought_chunks = [
            r.artifact_update.artifact.parts[0].text
            for r in responses
            if r.WhichOneof("payload") == "artifact_update"
            and r.artifact_update.artifact.parts
            and "cw_thought" in r.artifact_update.artifact.parts[0].metadata.fields
        ]
        assert len(thought_chunks) > 1
        assert thought_chunks[-1] == "思考：将请求拆解为 1 个节点。"
        assert "".join(thought_chunks[:-1]) == "思考：将请求拆解为 1 个节点。"


@pytest.mark.skip(reason="execute 暂为 plan-only，任务规划后立即完成，无 live 节点事件")
async def test_subscribe_replays_snapshot_then_live(tmp_path):
    delay = await start_fake_agent("delay", chunk_size=3)
    try:
        async with sdk_hub(
            tmp_path, "stream.db", plans=[_plan("delay")] * 2, streaming=True
        ) as (_app, http, client):
            await http.post("/v1/agents", json={"name": "delay", "card_url": delay.url})
            task_id = ""
            async for response in client.send_message(_send("hi")):
                if response.WhichOneof("payload") == "task":
                    task_id = response.task.id
            events = [
                event
                async for event in client.subscribe(
                    SubscribeToTaskRequest(id=task_id)
                )
            ]
    finally:
        await delay.stop()
    assert events[0].WhichOneof("payload") == "task"
    assert events[0].task.id == task_id
    live_kinds = [
        event.status_update.metadata.fields["kind"].string_value
        for event in events
        if event.WhichOneof("payload") == "status_update"
        and "kind" in event.status_update.metadata.fields
    ]
    assert any(kind.startswith("node.") for kind in live_kinds)
    last = events[-1]
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


async def test_subscribe_completed_task_raises(tmp_path, echo_agent):
    async with sdk_hub(
        tmp_path, "stream.db", plans=[_plan("echo")] * 2, streaming=True
    ) as (_app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        task_id = ""
        async for response in client.send_message(_send("hi")):
            if response.WhichOneof("payload") == "task":
                task_id = response.task.id
        await wait_for_task(client, task_id, {TaskState.TASK_STATE_COMPLETED})
        with pytest.raises(InvalidParamsError):
            async for _event in client.subscribe(
                SubscribeToTaskRequest(id=task_id)
            ):
                pass
