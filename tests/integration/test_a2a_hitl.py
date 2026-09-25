from a2a.types import GetTaskRequest, Message, Part, Role, SendMessageRequest, TaskState

from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.sim.fake_agent import start_fake_agent
from choirworks.tools.outcome_decision import OutcomeDecision
from tests.support.sdk import (
    answer_message,
    pending_intervention_id,
    question_ids_from_status,
    sdk_hub,
    task_artifact_text,
    task_nodes,
    task_state,
    wait_for_intervention_status,
    wait_for_pending_interventions,
    wait_for_question_parts,
    wait_for_task,
)


def _plan(agent_name: str, text: str = "任务") -> PlanDraft:
    return PlanDraft(
        nodes=[
            PlanNodeDraft(id="n1", name=agent_name, agent_name=agent_name, input={"text": text})
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
            OutcomeDecision(intent="deliver"),
        ],
    )
    async with sdk_hub(tmp_path, "hitl.db", settings=settings, llm=llm) as (
        _app,
        http,
        client,
    ):
        await http.post("/v1/agents", json={"name": "ask", "card_url": ask_agent.url})
        task_id = await _send_once(client, _message("请评估"))
        pending = await wait_for_task(client, task_id, {TaskState.TASK_STATE_INPUT_REQUIRED})
        assert task_state(pending).get("interventions")
        await _send_once(
            client,
            answer_message(
                pending_intervention_id(pending),
                "人工答复",
                task_id=task_id,
                context_id=pending.context_id,
                text="人工答复",
            ),
        )
        task = await wait_for_task(client, task_id, {TaskState.TASK_STATE_COMPLETED})
        assert "answered:" in task_artifact_text(task)


async def test_text_marker_needs_info_routes_to_human(tmp_path):
    writer = await start_fake_agent("needs_info_text", name="writer")
    try:
        settings = Settings(
            store={"db_path": tmp_path / "hitl.db"},
            a2a={"public_url": "http://test"},
            scheduler={"retry_backoff_seconds": 0.0},
        )
        from tests.support.fakes import FakeLLM

        llm = FakeLLM(
            structured_results=[
                _plan("writer", "写一份报告"),
                OutcomeDecision(intent="deliver"),
            ],
        )
        async with sdk_hub(tmp_path, "hitl.db", settings=settings, llm=llm) as (
            _app,
            http,
            client,
        ):
            await http.post("/v1/agents", json={"name": "writer", "card_url": writer.url})
            task_id = await _send_once(client, _message("写一份报告"))
            pending = await wait_for_task(client, task_id, {TaskState.TASK_STATE_INPUT_REQUIRED})
            node = task_state(pending)["nodes"][0]
            assert node["status"] == "input_required"
            assert "需要补充需求信息" in node["question"]
            assert any(item["status"] == "pending" for item in task_state(pending)["interventions"])
            await _send_once(
                client,
                answer_message(
                    pending_intervention_id(pending),
                    "补充需求",
                    task_id=task_id,
                    context_id=pending.context_id,
                    text="补充需求",
                ),
            )
            task = await wait_for_task(client, task_id, {TaskState.TASK_STATE_COMPLETED})
            assert "已获得补充信息" in task_artifact_text(task)
    finally:
        await writer.stop()


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
                OutcomeDecision(
                    intent="need_info",
                    target_agent="qa-engineer",
                    instruction="请协助确认技术细节",
                ),
                OutcomeDecision(intent="deliver"),
                OutcomeDecision(intent="deliver"),
            ],
        )
        async with sdk_hub(tmp_path, "hitl.db", settings=settings, llm=llm) as (
            _app,
            http,
            client,
        ):
            await http.post("/v1/agents", json={"name": "product-manager", "card_url": pm.url})
            await http.post("/v1/agents", json={"name": "qa-engineer", "card_url": qa.url})
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
            # Peer assistance must not surface as a human intervention/question.
            assert task_state(task).get("interventions", []) == []
            members = [
                member.get("name", member.get("agent_name"))
                for member in task_state(task).get("members", [])
            ]
            assert "qa-engineer" in members
    finally:
        await pm.stop()
        await qa.stop()


async def test_question_pushed_while_parallel_node_running(tmp_path, ask_agent):
    slow = await start_fake_agent("slow", name="slow")
    try:
        settings = Settings(
            store={"db_path": tmp_path / "parallel.db"},
            a2a={"public_url": "http://test"},
            scheduler={"retry_backoff_seconds": 0.0},
        )
        from tests.support.fakes import FakeLLM

        llm = FakeLLM(
            structured_results=[
                PlanDraft(
                    nodes=[
                        PlanNodeDraft(
                            id="n1", name="ask", agent_name="ask", input={"text": "请评估"}
                        ),
                        PlanNodeDraft(
                            id="n2", name="slow", agent_name="slow", input={"text": "慢任务"}
                        ),
                    ],
                ),
                OutcomeDecision(intent="need_info", question_type="confirm"),
                OutcomeDecision(intent="deliver"),
            ],
        )
        async with sdk_hub(tmp_path, "parallel.db", settings=settings, llm=llm) as (
            _app,
            http,
            client,
        ):
            await http.post("/v1/agents", json={"name": "ask", "card_url": ask_agent.url})
            await http.post("/v1/agents", json={"name": "slow", "card_url": slow.url})
            task_id = await _send_once(client, _message("并行任务"))
            pending = await wait_for_question_parts(client, task_id, 1)
            # 问题已推送，但并行节点仍在运行（未被排空阻塞）
            assert task_nodes(pending)["n2"]["status"] in {"submitted", "working"}
            assert pending.status.state == TaskState.TASK_STATE_INPUT_REQUIRED
            ids = question_ids_from_status(pending)
            assert ids
            await _send_once(
                client,
                answer_message(
                    ids[0],
                    True,
                    task_id=task_id,
                    context_id=pending.context_id,
                ),
            )
            task = await wait_for_task(
                client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30
            )
            assert task_nodes(task)["n2"]["status"] == "completed"
    finally:
        await slow.stop()


async def test_multiple_questions_aggregate_and_answer_separately(tmp_path, ask_agent):
    settings = Settings(
        store={"db_path": tmp_path / "multi.db"},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
    )
    from tests.support.fakes import FakeLLM

    llm = FakeLLM(
        structured_results=[
            PlanDraft(
                nodes=[
                    PlanNodeDraft(id="n1", name="ask", agent_name="ask", input={"text": "第一问"}),
                    PlanNodeDraft(id="n2", name="ask", agent_name="ask", input={"text": "第二问"}),
                ],
            ),
            OutcomeDecision(intent="deliver"),
            OutcomeDecision(intent="deliver"),
        ],
    )
    async with sdk_hub(tmp_path, "multi.db", settings=settings, llm=llm) as (_app, http, client):
        await http.post("/v1/agents", json={"name": "ask", "card_url": ask_agent.url})
        task_id = await _send_once(client, _message("两个问题"))
        await wait_for_pending_interventions(client, task_id, 2)
        pending = await wait_for_question_parts(client, task_id, 2)
        ids = question_ids_from_status(pending)
        assert len(ids) == 2

        await _send_once(
            client,
            answer_message(
                ids[0],
                "答一",
                task_id=task_id,
                context_id=pending.context_id,
                text="答一",
            ),
        )
        await wait_for_intervention_status(client, task_id, ids[0], "resolved")
        still = await client.get_task(GetTaskRequest(id=task_id))
        still_pending = [
            item for item in task_state(still)["interventions"] if item.get("status") == "pending"
        ]
        assert len(still_pending) == 1

        await _send_once(
            client,
            answer_message(
                str(still_pending[0]["id"]),
                "答二",
                task_id=task_id,
                context_id=pending.context_id,
                text="答二",
            ),
        )
        await wait_for_task(client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30)
