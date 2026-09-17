
import pytest
from a2a.types import Message, Part, Role, SendMessageRequest, TaskState

from choirworks.a2a.executor import AssistanceDecision
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.sim.fake_agent import start_fake_agent
from tests.support.sdk import (
    sdk_hub,
    task_artifact_text,
    task_metadata,
    task_nodes,
    wait_for_task,
)

pytestmark = pytest.mark.skip(reason="execute 暂为 plan-only，不派发/不处理干预")


def _plan(agent_name: str, text: str = "任务") -> PlanDraft:
    return PlanDraft(
        rationale="single",
        nodes=[
            PlanNodeDraft(
                id="n1", name=agent_name, agent_name=agent_name, input={"text": text}
            )
        ],
    )


def _message(text: str, *, task_id: str = "", context_id: str = "") -> SendMessageRequest:
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


async def test_llm_routes_to_human(tmp_path, ask_agent):
    settings = Settings(
        store={"db_path": tmp_path / "hitl.db"},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    from tests.support.fakes import FakeLLM

    llm = FakeLLM(
        structured_results=[
            _plan("ask"),
            AssistanceDecision(action="human"),
        ],
    )
    async with sdk_hub(tmp_path, "hitl.db", settings=settings, llm=llm) as (
        _app,
        http,
        client,
    ):
        await http.post("/v1/agents", json={"name": "ask", "card_url": ask_agent.url})
        task_id = await _send_once(client, _message("请评估"))
        pending = await wait_for_task(
            client, task_id, {TaskState.TASK_STATE_INPUT_REQUIRED}
        )
        assert task_metadata(pending).get("interventions")
        await _send_once(
            client, _message("人工答复", task_id=task_id, context_id=pending.context_id)
        )
        task = await wait_for_task(
            client, task_id, {TaskState.TASK_STATE_COMPLETED}
        )
        assert "人工答复" in task_artifact_text(task)


async def test_llm_routes_to_peer_agent(tmp_path):
    pm = await start_fake_agent("collaborate", name="product-manager")
    qa = await start_fake_agent("assist", name="qa-engineer")
    try:
        settings = Settings(
            store={"db_path": tmp_path / "hitl.db"},
            a2a={"public_url": "http://test"},
            scheduler={"retry_backoff_seconds": 0.0},
        )
        from tests.support.fakes import FakeLLM

        llm = FakeLLM(
            structured_results=[
                _plan("product-manager", "请协调协作"),
                AssistanceDecision(
                    action="peer",
                    agent_name="qa-engineer",
                    instruction="请协助确认技术细节",
                ),
            ],
        )
        async with sdk_hub(tmp_path, "hitl.db", settings=settings, llm=llm) as (
            _app,
            http,
            client,
        ):
            await http.post(
                "/v1/agents", json={"name": "product-manager", "card_url": pm.url}
            )
            await http.post(
                "/v1/agents", json={"name": "qa-engineer", "card_url": qa.url}
            )
            task_id = await _send_once(client, _message("请协调协作"))
            task = await wait_for_task(
                client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30
            )
            nodes = task_nodes(task)
            helpers = [node for node in nodes.values() if node.get("derived")]
            assert helpers, nodes
            assert helpers[0]["agent_name"] == "qa-engineer"
            assert helpers[0]["status"] == "completed"
            assert nodes["n1"]["status"] == "completed"
            members = [
                member.get("name", member.get("agent_name"))
                for member in task_metadata(task).get("members", [])
            ]
            assert "qa-engineer" in members
    finally:
        await pm.stop()
        await qa.stop()
