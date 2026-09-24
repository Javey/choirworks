import asyncio

import pytest
from a2a.types import (
    CancelTaskRequest,
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageRequest,
    TaskState,
)
from a2a.utils.errors import TaskNotCancelableError, TaskNotFoundError

from choirworks.core.planner import PlanDraft, PlanNodeDraft
from tests.support.sdk import context_nodes, sdk_hub, task_nodes, wait_for_task


def _plan(agent_name: str) -> PlanDraft:
    return PlanDraft(
        nodes=[
            PlanNodeDraft(id="n1", name=agent_name, agent_name=agent_name, input={"text": "hi"})
        ],
    )


def _send(text: str, *, task_id: str = "", context_id: str = "") -> SendMessageRequest:
    return SendMessageRequest(
        message=Message(
            message_id=f"m-{text[:6]}",
            role=Role.ROLE_USER,
            parts=[Part(text=text)],
            task_id=task_id,
            context_id=context_id,
        )
    )


async def _send_once(client, request) -> str:
    task_id = ""
    async for response in client.send_message(request):
        if response.WhichOneof("payload") == "task":
            task_id = response.task.id
    return task_id


async def test_get_task_matches_plan_snapshot(tmp_path, echo_agent):
    async with sdk_hub(tmp_path, "read.db", plans=[_plan("echo")] * 2) as (app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        task_id = await _send_once(client, _send("hello"))
        task = await wait_for_task(client, task_id, {TaskState.TASK_STATE_COMPLETED})
        assert task.id == task_id
        assert task.context_id
        nodes = await context_nodes(app, task.context_id)
        assert nodes["n1"]["status"] == "completed"
        assert nodes["n1"]["output"]


async def test_conversation_endpoint_returns_context_and_tasks(tmp_path, echo_agent):
    async with sdk_hub(tmp_path, "read.db", plans=[_plan("echo")] * 2) as (_app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        task_id = await _send_once(client, _send("hello"))
        task = await wait_for_task(client, task_id, {TaskState.TASK_STATE_COMPLETED})

        response = await http.get(f"/v1/conversations/{task.context_id}")
        assert response.status_code == 200
        body = response.json()
        assert body["id"] == task.context_id
        assert body["context"]["nodes"][0]["id"] == "n1"
        assert [item["id"] for item in body["tasks"]] == [task_id]

        listed = (await http.get("/v1/conversations")).json()
        [session] = [item for item in listed if item["id"] == task.context_id]
        assert session["task_count"] == 1
        assert session["last_status"] == "completed"
        assert session["created_at"]
        assert session["updated_at"]


async def test_get_task_unknown_raises(tmp_path):
    async with sdk_hub(tmp_path, "read.db") as (_app, _http, client):
        with pytest.raises(TaskNotFoundError):
            await client.get_task(GetTaskRequest(id="missing"))


async def test_cancel_unknown_task_raises(tmp_path):
    async with sdk_hub(tmp_path, "read.db") as (_app, _http, client):
        with pytest.raises(TaskNotFoundError):
            await client.cancel_task(CancelTaskRequest(id="missing"))


async def test_cancel_running_task_marks_canceled(tmp_path):
    from choirworks.sim.fake_agent import start_fake_agent

    slow = await start_fake_agent("slow")
    try:
        async with sdk_hub(tmp_path, "read.db", plans=[_plan("slow")] * 2) as (_app, http, client):
            await http.post("/v1/agents", json={"name": "slow", "card_url": slow.url})
            task_id = await _send_once(client, _send("hi"))
            for _ in range(200):
                task = await client.get_task(GetTaskRequest(id=task_id))
                if task_nodes(task)["n1"]["status"] in {
                    "submitted",
                    "working",
                }:
                    break
                await asyncio.sleep(0.05)
            canceled = await client.cancel_task(CancelTaskRequest(id=task_id))
            assert canceled.status.state == TaskState.TASK_STATE_CANCELED
    finally:
        await slow.stop()


async def test_cancel_terminal_task_raises(tmp_path, echo_agent):
    async with sdk_hub(tmp_path, "read.db", plans=[_plan("echo")] * 2) as (_app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        task_id = await _send_once(client, _send("hello"))
        await wait_for_task(client, task_id, {TaskState.TASK_STATE_COMPLETED})
        with pytest.raises(TaskNotCancelableError):
            await client.cancel_task(CancelTaskRequest(id=task_id))
