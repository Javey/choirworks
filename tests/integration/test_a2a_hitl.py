
from a2a.types import Message, Part, Role, SendMessageRequest, TaskState

from choirworks.a2a.executor import PeerChoice
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




async def test_auto_llm_resolves_intervention(tmp_path, ask_agent):
    settings = Settings(
        store={"db_path": tmp_path / "hitl.db"},
        a2a={"public_url": "http://test"},
        policies={"default": "auto_llm"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    from tests.support.fakes import FakeLLM

    llm = FakeLLM(structured_results=[_plan("ask")] * 2, text_results=["自动答复"])
    async with sdk_hub(tmp_path, "hitl.db", settings=settings, llm=llm) as (
        _app,
        http,
        client,
    ):
        await http.post("/v1/agents", json={"name": "ask", "card_url": ask_agent.url})
        task_id = await _send_once(client, _message("请评估"))
        task = await wait_for_task(client, task_id, {TaskState.TASK_STATE_COMPLETED})
        assert "自动答复" in task_artifact_text(task)
        interventions = task_metadata(task).get("interventions", [])
        assert any(item.get("responder") == "auto_llm" for item in interventions)


async def test_human_intervention_awaits_answer(tmp_path, ask_agent):
    settings = Settings(
        store={"db_path": tmp_path / "hitl.db"},
        a2a={"public_url": "http://test"},
        policies={"default": "human", "timeout_seconds": 30},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    async with sdk_hub(
        tmp_path, "hitl.db", settings=settings, plans=[_plan("ask")] * 2
    ) as (_app, http, client):
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


async def test_peer_agent_spawns_helper_and_resumes(tmp_path):
    researcher = await start_fake_agent("collaborate", name="researcher")
    analyst = await start_fake_agent("assist", name="analyst")
    try:
        settings = Settings(
            store={"db_path": tmp_path / "hitl.db"},
            a2a={"public_url": "http://test"},
            policies={
                "default": "auto_llm",
                "overrides": [{"agent_name": "researcher", "policy": "peer_agent"}],
            },
            scheduler={"retry_backoff_seconds": 0.0},
        )
        from tests.support.fakes import FakeLLM

        llm = FakeLLM(
            structured_results=[
                _plan("researcher", "请协调协作"),
                PeerChoice(agent_name="analyst", instruction="请协助确认技术细节"),
            ],
            text_results=["fallback"],
        )
        async with sdk_hub(tmp_path, "hitl.db", settings=settings, llm=llm) as (
            _app,
            http,
            client,
        ):
            await http.post(
                "/v1/agents", json={"name": "researcher", "card_url": researcher.url}
            )
            await http.post(
                "/v1/agents", json={"name": "analyst", "card_url": analyst.url}
            )
            task_id = await _send_once(client, _message("请协调协作"))
            task = await wait_for_task(
                client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30
            )
            nodes = task_nodes(task)
            helpers = [node for node in nodes.values() if node.get("derived")]
            assert helpers, nodes
            assert helpers[0]["agent_name"] == "analyst"
            assert helpers[0]["status"] == "completed"
            assert nodes["n1"]["status"] == "completed"
            members = [
                member.get("name", member.get("agent_name"))
                for member in task_metadata(task).get("members", [])
            ]
            assert "analyst" in members
    finally:
        await researcher.stop()
        await analyst.stop()
