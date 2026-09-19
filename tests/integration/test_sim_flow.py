"""End-to-end smoke test of the offline simulation flow.

Exercises the deterministic sim planner plus fake agents through the real
orchestrator: plan -> dispatch -> handoff -> completion.
"""

from a2a.types import Message, Part, Role, SendMessageRequest, TaskState

from choirworks.config import Settings
from choirworks.core.llm import LiteLLMClient
from choirworks.sim.fake_agent import start_fake_agent
from choirworks.sim.litellm_mock import sim_acompletion
from tests.support.sdk import sdk_hub, task_nodes, wait_for_task


def _message(text: str) -> SendMessageRequest:
    return SendMessageRequest(
        message=Message(
            message_id="m-1", role=Role.ROLE_USER, parts=[Part(text=text)]
        )
    )


async def test_sim_plan_chain_hands_off_results(tmp_path):
    pm = await start_fake_agent("collaborate", name="product-manager")
    dev = await start_fake_agent("inquire", name="developer")
    try:
        settings = Settings(
            store={"db_path": tmp_path / "sim.db"},
            a2a={"public_url": "http://test"},
            scheduler={"retry_backoff_seconds": 0.0},
        )
        llm = LiteLLMClient(model="sim", completion_fn=sim_acompletion)
        async with sdk_hub(
            tmp_path, "sim.db", settings=settings, llm=llm
        ) as (app, http, client):
            await http.post(
                "/v1/agents", json={"name": "product-manager", "card_url": pm.url}
            )
            await http.post(
                "/v1/agents", json={"name": "developer", "card_url": dev.url}
            )
            task_id = ""
            async for response in client.send_message(_message("帮我调研并写一份报告")):
                if response.WhichOneof("payload") == "task":
                    task_id = response.task.id
            task = await wait_for_task(
                client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30
            )
            nodes = task_nodes(task)
            assert nodes["n1"]["status"] == "completed"
            assert nodes["n2"]["status"] == "completed"
            # The developer received the product-manager output via handoff.
            assert "调研结果" in nodes["n2"]["output"]
    finally:
        await pm.stop()
        await dev.stop()
