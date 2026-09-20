import asyncio
import uuid

import pytest
from a2a.helpers import new_text_message
from a2a.types import GetTaskRequest, SendMessageRequest, TaskState

from choirworks.config import Settings
from choirworks.core.llm import LiteLLMClient
from choirworks.sim.litellm_mock import sim_acompletion
from choirworks.sim.runner import EXAMPLES, SIM_AGENTS
from tests.support.sdk import sdk_hub, task_metadata, task_nodes, wait_for_task


@pytest.mark.parametrize(
    ("example_index", "initial_agents"),
    [
        (0, ["product-manager", "developer"]),
        (1, ["product-manager", "developer"]),
        (2, ["developer", "code-reviewer"]),
        (3, ["finance-analyst"]),
        (4, ["approval-manager", "finance-analyst"]),
        (5, ["approval-manager", "finance-analyst"]),
        (6, ["auditor", "finance-analyst"]),
    ],
)
async def test_sim_example_through_real_planner_and_a2a(tmp_path, example_index, initial_agents):
    settings = Settings(
        store={"db_path": tmp_path / "sim.db"},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
        sim={
            "start_agents": True,
            "agents": [{"name": name, "behavior": behavior} for name, behavior in SIM_AGENTS],
            "chunk_size": 2,
            "chunk_delay": 0,
        },
    )
    llm = LiteLLMClient(model="sim", completion_fn=sim_acompletion)
    async with sdk_hub(tmp_path, settings=settings, llm=llm) as (_, _, client):
        initial = None
        message = new_text_message(EXAMPLES[example_index])
        message.message_id = uuid.uuid4().hex
        async for response in client.send_message(SendMessageRequest(message=message)):
            if response.HasField("task"):
                initial = response.task
        assert initial is not None
        nodes = task_nodes(initial)
        assert [nodes[key]["agent_name"] for key in ("n1", "n2") if key in nodes] == initial_agents
        assert nodes["n1"]["input_text"] == EXAMPLES[example_index]
        assert "For context:" not in " ".join(
            part.text for msg in initial.history for part in msg.parts
        )

        if example_index == 2:
            async with asyncio.timeout(10):
                while True:
                    pending = await client.get_task(GetTaskRequest(id=initial.id))
                    interventions = task_metadata(pending).get("interventions", [])
                    if any(item["status"] == "pending" for item in interventions):
                        break
                    await asyncio.sleep(0.05)
            assert pending.status.state == TaskState.TASK_STATE_INPUT_REQUIRED
            reply = new_text_message(
                "同意采用方案", task_id=initial.id, context_id=initial.context_id
            )
            async for _ in client.send_message(SendMessageRequest(message=reply)):
                pass

        completed = await wait_for_task(client, initial.id, {TaskState.TASK_STATE_COMPLETED})
        output = "\n".join(
            "".join(part.text for part in artifact.parts if part.HasField("text"))
            for artifact in completed.artifacts
        )
        assert output
        assert "Available agents:" not in output
        final_nodes = task_nodes(completed)
        if example_index == 0:
            assert any(node.get("derived") for node in final_nodes.values())
        elif example_index == 2:
            assert "同意采用方案" in output
        elif example_index in (4, 5):
            assert final_nodes["n1"]["attempt"] == 2
        elif example_index == 6:
            assert task_metadata(completed)["plan"]["version"] == 2
            assert all(node["agent_name"] != "auditor" for node in final_nodes.values())
