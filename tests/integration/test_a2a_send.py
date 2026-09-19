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
from a2a.utils.errors import InvalidParamsError, TaskNotFoundError

from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.sim.fake_agent import start_fake_agent
from tests.support.sdk import (
    context_nodes,
    context_state,
    sdk_hub,
    task_metadata,
    task_nodes,
    wait_for_task,
)


def _plan(agent_name: str, text: str = "问题") -> PlanDraft:
    return PlanDraft(
        nodes=[
            PlanNodeDraft(
                id="n1", name=agent_name, agent_name=agent_name, input={"text": text}
            )
        ],
    )


def _message(
    text: str, *, task_id: str = "", context_id: str = ""
) -> SendMessageRequest:
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


async def test_send_creates_task_with_plan(tmp_path, echo_agent):
    async with sdk_hub(
        tmp_path, "send.db", plans=[_plan("echo")] * 2
    ) as (app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        task_id = await _send_once(client, _message("请评估这个问题"))
        task = await wait_for_task(client, task_id, {TaskState.TASK_STATE_COMPLETED})
        assert task.id == task_id
        assert task.context_id
        nodes = await context_nodes(app, task.context_id)
        assert nodes["n1"]["agent_name"] == "echo"
        assert nodes["n1"]["status"] == "pending"


async def test_send_with_context_creates_followup_task(tmp_path, echo_agent):
    async with sdk_hub(
        tmp_path, "send.db", plans=[_plan("echo")] * 4
    ) as (app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        first_id = await _send_once(client, _message("第一个任务"))
        first = await wait_for_task(client, first_id, {TaskState.TASK_STATE_COMPLETED})
        assert app.state.executor._sessions == {}
        first_version = (await context_state(app, first.context_id))["plan_version"]
        assert first_version == 2
        second_id = await _send_once(
            client, _message("第二个任务", context_id=first.context_id)
        )
        second = await wait_for_task(
            client, second_id, {TaskState.TASK_STATE_COMPLETED}
        )
        assert second.id != first.id
        assert second.context_id == first.context_id
        assert app.state.executor._sessions == {}
        # The second turn reloads the canonical contexts row, so the plan
        # version continues from the first turn instead of restarting at 1.
        assert (
            await context_state(app, second.context_id)
        )["plan_version"] == first_version + 1


@pytest.mark.skip(reason="execute 暂为 plan-only，不派发/不处理干预")
async def test_send_answers_pending_intervention(tmp_path, ask_agent):
    from choirworks.a2a.executor import AssistanceDecision

    settings = Settings(
        store={"db_path": tmp_path / "send.db"},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    async with sdk_hub(
        tmp_path,
        "send.db",
        settings=settings,
        plans=[_plan("ask", "请评估"), AssistanceDecision(action="human")],
    ) as (_app, http, client):
        await http.post("/v1/agents", json={"name": "ask", "card_url": ask_agent.url})
        task_id = await _send_once(client, _message("请评估"))
        pending = await wait_for_task(
            client, task_id, {TaskState.TASK_STATE_INPUT_REQUIRED}
        )
        interventions = task_metadata(pending).get("interventions", [])
        assert any(item.get("status") == "pending" for item in interventions)
        resumed_id = await _send_once(
            client, _message("这是答复", task_id=task_id, context_id=pending.context_id)
        )
        assert resumed_id == task_id
        resumed = await wait_for_task(
            client, task_id, {TaskState.TASK_STATE_COMPLETED}
        )
        assert "这是答复" in task_nodes(resumed)["n1"]["output"]
        history_text = " ".join(
            part.text for msg in resumed.history for part in msg.parts if part.HasField("text")
        )
        assert "这是答复" in history_text


@pytest.mark.skip(reason="execute 暂为 plan-only，不派发/不排队")
async def test_send_to_running_task_queues_message(tmp_path):
    slow = await start_fake_agent("slow")
    try:
        async with sdk_hub(
            tmp_path, "send.db", plans=[_plan("slow")] * 2
        ) as (_app, http, client):
            await http.post("/v1/agents", json={"name": "slow", "card_url": slow.url})
            task_id = await _send_once(client, _message("开始"))
            for _ in range(200):
                task = await client.get_task(GetTaskRequest(id=task_id))
                if task_nodes(task)["n1"]["status"] in {"dispatched", "working"}:
                    break
                await asyncio.sleep(0.05)
            second_id = await _send_once(
                client, _message("补充说明", task_id=task_id, context_id=task.context_id)
            )
            assert second_id == task_id
            task = await client.get_task(GetTaskRequest(id=task_id))
            queue = task_metadata(task).get("queue", {})
            assert "补充说明" in queue["n1"][0]["text"]
            await client.cancel_task(CancelTaskRequest(id=task_id))
    finally:
        await slow.stop()


async def test_plan_failure_persists_context_state(tmp_path, echo_agent):
    async with sdk_hub(
        tmp_path,
        "send.db",
        plans=[ValueError("bad plan")] * 3,
    ) as (app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        task_id = await _send_once(client, _message("无法规划"))
        task = await wait_for_task(client, task_id, {TaskState.TASK_STATE_FAILED})
        assert app.state.executor._sessions == {}

        state = await context_state(app, task.context_id)
        assert state["nodes"] == []
        # start_new_plan bumped the version before planning failed.
        assert state["plan_version"] == 2


async def test_send_empty_text_raises(tmp_path, echo_agent):
    async with sdk_hub(
        tmp_path, "send.db", plans=[_plan("echo")] * 2
    ) as (_app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        with pytest.raises(InvalidParamsError):
            await _send_once(
                client,
                SendMessageRequest(
                    message=Message(message_id="m-1", role=Role.ROLE_USER, parts=[])
                ),
            )


async def test_send_unknown_task_raises(tmp_path):
    async with sdk_hub(tmp_path, "send.db") as (_app, _http, client):
        with pytest.raises(TaskNotFoundError):
            await _send_once(client, _message("继续", task_id="missing"))
