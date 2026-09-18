import asyncio

import pytest
from a2a.types import CancelTaskRequest, GetTaskRequest, Message, Part, Role, SendMessageRequest
from google.protobuf.json_format import ParseDict

from choirworks.a2a.room import A2A_ROOM_URI
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.sim.fake_agent import start_fake_agent
from tests.support.sdk import sdk_hub, task_nodes

pytestmark = pytest.mark.skip(reason="execute 暂为 plan-only，不派发/不排队")


def _plan(agent_name: str) -> PlanDraft:
    return PlanDraft(
        nodes=[
            PlanNodeDraft(
                id="n1", name=agent_name, agent_name=agent_name, input={"text": "开始"}
            )
        ],
    )


async def _send_once(client, request) -> str:
    task_id = ""
    async for response in client.send_message(request):
        if response.WhichOneof("payload") == "task":
            task_id = response.task.id
    return task_id


async def test_interrupt_cancels_active_node_and_starts_followup(tmp_path):
    slow = await start_fake_agent("slow")
    try:
        async with sdk_hub(
            tmp_path, "queue.db", plans=[_plan("slow")] * 2
        ) as (_app, http, client):
            await http.post("/v1/agents", json={"name": "slow", "card_url": slow.url})
            task_id = await _send_once(
                client,
                SendMessageRequest(
                    message=Message(
                        message_id="m-1",
                        role=Role.ROLE_USER,
                        parts=[Part(text="开始")],
                    )
                ),
            )
            for _ in range(200):
                task = await client.get_task(GetTaskRequest(id=task_id))
                if task_nodes(task)["n1"]["status"] in {"dispatched", "working"}:
                    break
                await asyncio.sleep(0.05)

            interrupt = Message(
                message_id="m-2",
                role=Role.ROLE_USER,
                parts=[Part(text="换个方向")],
                task_id=task_id,
                context_id=task.context_id,
            )
            ParseDict(
                {A2A_ROOM_URI: {"interrupt": True, "quote_id": "n1"}},
                interrupt.metadata,
            )
            from a2a.types import SendMessageRequest as SMR

            second_id = await _send_once(client, SMR(message=interrupt))
            assert second_id == task_id
            task = await client.get_task(GetTaskRequest(id=task_id))
            nodes = task_nodes(task)
            assert nodes["n1"]["status"] == "canceled"
            derived = [node for node in nodes.values() if node.get("derived")]
            assert derived
            await client.cancel_task(CancelTaskRequest(id=task_id))
    finally:
        await slow.stop()
