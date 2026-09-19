from a2a.types import GetTaskRequest, Message, Part, Role, SendMessageRequest, TaskState
from google.protobuf.json_format import ParseDict

from choirworks.a2a.room import A2A_ROOM_URI
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from tests.support.sdk import context_state, sdk_hub, wait_for_task


def _plan(agent_name: str) -> PlanDraft:
    return PlanDraft(
        nodes=[
            PlanNodeDraft(
                id="n1", name=agent_name, agent_name=agent_name, input={"text": "hi"}
            )
        ],
    )


def _message(
    text: str, *, context_id: str = "", sender: str | None = None
) -> SendMessageRequest:
    message = Message(
        message_id=f"m-{text}",
        role=Role.ROLE_USER,
        parts=[Part(text=text)],
        context_id=context_id,
    )
    if sender is not None:
        ParseDict({A2A_ROOM_URI: {"sender": sender}}, message.metadata)
    return SendMessageRequest(message=message)


async def _send_once(client, request) -> str:
    task_id = ""
    async for response in client.send_message(request):
        if response.WhichOneof("payload") == "task":
            task_id = response.task.id
    return task_id


async def test_rewind_hides_turn_and_restores_state(tmp_path, echo_agent):
    async with sdk_hub(
        tmp_path, "rewind.db", plans=[_plan("echo")] * 6
    ) as (app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        first_id = await _send_once(client, _message("第一轮"))
        first = await wait_for_task(client, first_id, {TaskState.TASK_STATE_COMPLETED})
        second_id = await _send_once(
            client, _message("第二轮", context_id=first.context_id)
        )
        await wait_for_task(client, second_id, {TaskState.TASK_STATE_COMPLETED})
        assert (await context_state(app, first.context_id))["plan_version"] == 3

        rewind = await http.post(
            f"/v1/conversations/{first.context_id}/rewind",
            json={"task_id": second_id},
        )
        assert rewind.status_code == 200
        body = rewind.json()
        assert [task["id"] for task in body["tasks"]] == [first_id]
        assert body["context"]["plan_version"] == 2

        # Hidden tasks stay in the A2A store.
        stored = await client.get_task(GetTaskRequest(id=second_id))
        assert stored.id == second_id

        # A new turn continues from the restored state.
        third_id = await _send_once(
            client, _message("第三轮", context_id=first.context_id)
        )
        await wait_for_task(client, third_id, {TaskState.TASK_STATE_COMPLETED})
        assert (await context_state(app, first.context_id))["plan_version"] == 3

        detail = (await http.get(f"/v1/conversations/{first.context_id}")).json()
        assert [task["id"] for task in detail["tasks"]] == [first_id, third_id]

        sessions = (await http.get("/v1/conversations")).json()
        [session] = [item for item in sessions if item["id"] == first.context_id]
        assert session["task_count"] == 2
        assert session["last_status"] == "completed"

        # Rewinding the third turn walks back past the hidden second turn.
        chain = await http.post(
            f"/v1/conversations/{first.context_id}/rewind",
            json={"task_id": third_id},
        )
        assert chain.status_code == 200
        chained = chain.json()
        assert [task["id"] for task in chained["tasks"]] == [first_id]
        assert chained["context"]["plan_version"] == 2


async def test_rewind_rejects_hidden_target(tmp_path, echo_agent):
    async with sdk_hub(
        tmp_path, "rewind.db", plans=[_plan("echo")] * 4
    ) as (_app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        first_id = await _send_once(client, _message("第一轮"))
        first = await wait_for_task(client, first_id, {TaskState.TASK_STATE_COMPLETED})
        second_id = await _send_once(
            client, _message("第二轮", context_id=first.context_id)
        )
        await wait_for_task(client, second_id, {TaskState.TASK_STATE_COMPLETED})

        first_rewind = await http.post(
            f"/v1/conversations/{first.context_id}/rewind",
            json={"task_id": second_id},
        )
        assert first_rewind.status_code == 200

        again = await http.post(
            f"/v1/conversations/{first.context_id}/rewind",
            json={"task_id": second_id},
        )
        assert again.status_code == 409


async def test_rewind_rejects_unknown_task_and_context(tmp_path, echo_agent):
    async with sdk_hub(
        tmp_path, "rewind.db", plans=[_plan("echo")] * 2
    ) as (_app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        first_id = await _send_once(client, _message("第一轮"))
        first = await wait_for_task(client, first_id, {TaskState.TASK_STATE_COMPLETED})

        missing_context = await http.post(
            "/v1/conversations/nope/rewind", json={"task_id": first_id}
        )
        assert missing_context.status_code == 404

        missing_task = await http.post(
            f"/v1/conversations/{first.context_id}/rewind",
            json={"task_id": "missing"},
        )
        assert missing_task.status_code == 404

        no_task_id = await http.post(
            f"/v1/conversations/{first.context_id}/rewind", json={}
        )
        assert no_task_id.status_code == 400


async def test_rewind_rejects_non_human_turn(tmp_path, echo_agent):
    async with sdk_hub(
        tmp_path, "rewind.db", plans=[_plan("echo")] * 2
    ) as (_app, http, client):
        await http.post("/v1/agents", json={"name": "echo", "card_url": echo_agent.url})
        task_id = await _send_once(client, _message("来自 agent", sender="echo"))
        task = await wait_for_task(client, task_id, {TaskState.TASK_STATE_COMPLETED})

        response = await http.post(
            f"/v1/conversations/{task.context_id}/rewind",
            json={"task_id": task_id},
        )
        assert response.status_code == 400
