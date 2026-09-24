import asyncio

from a2a.types import (
    GetTaskRequest,
    Message,
    Part,
    Role,
    SendMessageRequest,
    TaskState,
)

from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from choirworks.orchestration.planning.patch import PatchNode, PlanPatch
from choirworks.sim.fake_agent import start_fake_agent
from choirworks.tools.outcome_decision import OutcomeDecision
from tests.support.fakes import FakeLLM
from tests.support.sdk import answer_message, sdk_hub, task_nodes, task_state, wait_for_task


def _settings(db_path) -> Settings:
    return Settings(
        store={"db_path": db_path},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
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


async def test_revise_patch_invalidates_and_adds(tmp_path):
    writer = await start_fake_agent("echo", name="writer")
    designer = await start_fake_agent("echo", name="designer")
    try:
        llm = FakeLLM(
            structured_results=[
                PlanDraft(
                    nodes=[
                        PlanNodeDraft(
                            id="n1",
                            name="writer",
                            agent_name="writer",
                            input={"text": "写草稿"},
                        ),
                        PlanNodeDraft(
                            id="n2",
                            name="writer",
                            agent_name="writer",
                            input={"text": "润色"},
                            deps=["n1"],
                        ),
                    ],
                ),
                OutcomeDecision(
                    intent="revise",
                    question="不再需要润色，改由 designer 出原型",
                    patch=PlanPatch(
                        add=[
                            PatchNode(
                                agent_name="designer",
                                instruction="出原型",
                                deps=["n1"],
                            )
                        ],
                        invalidate=["n2"],
                        reason="方向调整",
                    ),
                ),
                OutcomeDecision(intent="deliver"),
            ],
        )
        async with sdk_hub(
            tmp_path, "revise.db", settings=_settings(tmp_path / "revise.db"), llm=llm
        ) as (app, http, client):
            await http.post("/v1/agents", json={"name": "writer", "card_url": writer.url})
            await http.post("/v1/agents", json={"name": "designer", "card_url": designer.url})
            task_id = await _send_once(client, _message("写一份材料"))
            task = await wait_for_task(
                client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30
            )
            state = task_state(task)
            nodes = {item["id"]: item for item in state["nodes"]}
            assert nodes["n1"]["status"] == "completed"
            assert nodes["n2"]["status"] == "invalidated"
            assert nodes["x1"]["status"] == "completed"
            assert nodes["x1"]["agent_name"] == "designer"
            members = {member.get("name", member.get("agent_name")) for member in state["members"]}
            assert "designer" in members
    finally:
        await writer.stop()
        await designer.stop()


async def _wait_for_cancel_request(client, task_id, timeout_seconds: float = 10.0):
    """Poll until a pending confirm_cancel intervention appears."""
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while asyncio.get_event_loop().time() < deadline:
        task = await client.get_task(GetTaskRequest(id=task_id))
        for intervention in task_state(task).get("interventions", []):
            if intervention["kind"] == "confirm_cancel" and intervention["status"] == "pending":
                return task, intervention
        await asyncio.sleep(0.05)
    raise AssertionError("no pending confirm_cancel intervention")


async def test_revise_in_flight_requires_confirmation(tmp_path):
    planner = await start_fake_agent("echo", name="planner")
    slow = await start_fake_agent("slow", name="slow")
    try:
        llm = FakeLLM(
            structured_results=[
                PlanDraft(
                    nodes=[
                        PlanNodeDraft(
                            id="n1",
                            name="planner",
                            agent_name="planner",
                            input={"text": "快速任务"},
                        ),
                        PlanNodeDraft(
                            id="n2",
                            name="slow",
                            agent_name="slow",
                            input={"text": "慢任务"},
                        ),
                    ],
                ),
                OutcomeDecision(
                    intent="revise",
                    question="建议作废慢任务",
                    patch=PlanPatch(invalidate=["n2"], reason="不再需要慢任务"),
                ),
            ],
        )
        async with sdk_hub(
            tmp_path, "revise.db", settings=_settings(tmp_path / "revise.db"), llm=llm
        ) as (app, http, client):
            await http.post("/v1/agents", json={"name": "planner", "card_url": planner.url})
            await http.post("/v1/agents", json={"name": "slow", "card_url": slow.url})
            task_id = await _send_once(client, _message("开始"))
            pending, confirm = await _wait_for_cancel_request(client, task_id)
            assert confirm["target_node_id"] == "n2"
            await _send_once(
                client,
                answer_message(
                    str(confirm["id"]),
                    True,
                    task_id=task_id,
                    context_id=pending.context_id,
                ),
            )
            task = await wait_for_task(
                client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30
            )
            nodes = task_nodes(task)
            assert nodes["n1"]["status"] == "completed"
            assert nodes["n2"]["status"] == "canceled"
    finally:
        await planner.stop()
        await slow.stop()


async def test_confirm_cancel_expires_when_target_finishes(tmp_path):
    planner = await start_fake_agent("echo", name="planner")
    worker = await start_fake_agent("delay", name="worker")
    try:
        llm = FakeLLM(
            structured_results=[
                PlanDraft(
                    nodes=[
                        PlanNodeDraft(
                            id="n1",
                            name="planner",
                            agent_name="planner",
                            input={"text": "快速任务"},
                        ),
                        PlanNodeDraft(
                            id="n2",
                            name="worker",
                            agent_name="worker",
                            input={"text": "短任务"},
                        ),
                    ],
                ),
                OutcomeDecision(
                    intent="revise",
                    question="建议作废短任务",
                    patch=PlanPatch(invalidate=["n2"], reason="不再需要"),
                ),
                OutcomeDecision(intent="deliver"),
            ],
        )
        async with sdk_hub(
            tmp_path, "revise.db", settings=_settings(tmp_path / "revise.db"), llm=llm
        ) as (app, http, client):
            await http.post("/v1/agents", json={"name": "planner", "card_url": planner.url})
            await http.post("/v1/agents", json={"name": "worker", "card_url": worker.url})
            task_id = await _send_once(client, _message("开始"))
            await _wait_for_cancel_request(client, task_id)
            task = await wait_for_task(
                client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30
            )
            state = task_state(task)
            nodes = {item["id"]: item for item in state["nodes"]}
            assert nodes["n2"]["status"] == "completed"
            confirms = [item for item in state["interventions"] if item["kind"] == "confirm_cancel"]
            assert confirms and confirms[0]["status"] == "expired"
    finally:
        await planner.stop()
        await worker.stop()


async def test_failed_node_repaired_by_patch(tmp_path):
    flaky = await start_fake_agent("fail", name="flaky")
    writer = await start_fake_agent("echo", name="writer")
    try:
        llm = FakeLLM(
            structured_results=[
                PlanDraft(
                    nodes=[
                        PlanNodeDraft(
                            id="n1",
                            name="flaky",
                            agent_name="flaky",
                            input={"text": "执行"},
                        )
                    ],
                ),
                OutcomeDecision(
                    intent="revise",
                    question="替换失败节点",
                    patch=PlanPatch(
                        add=[PatchNode(agent_name="writer", instruction="接替完成")],
                        reason="节点失败",
                    ),
                ),
                OutcomeDecision(intent="deliver"),
            ],
        )
        async with sdk_hub(
            tmp_path, "revise.db", settings=_settings(tmp_path / "revise.db"), llm=llm
        ) as (app, http, client):
            await http.post("/v1/agents", json={"name": "flaky", "card_url": flaky.url})
            await http.post("/v1/agents", json={"name": "writer", "card_url": writer.url})
            task_id = await _send_once(client, _message("完成工作"))
            task = await wait_for_task(
                client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30
            )
            nodes = task_nodes(task)
            assert nodes["n1"]["status"] == "invalidated"
            assert nodes["x1"]["status"] == "completed"
            assert nodes["x1"]["agent_name"] == "writer"
    finally:
        await flaky.stop()
        await writer.stop()


async def test_revise_limit_stops_loop(tmp_path):
    writer = await start_fake_agent("echo", name="writer")
    try:
        revise_patch = PlanPatch(
            add=[PatchNode(agent_name="writer", instruction="再写一遍")],
            reason="不满意",
        )
        llm = FakeLLM(
            structured_results=[
                PlanDraft(
                    nodes=[
                        PlanNodeDraft(
                            id="n1",
                            name="writer",
                            agent_name="writer",
                            input={"text": "写草稿"},
                        ),
                    ],
                ),
                *[OutcomeDecision(intent="revise", patch=revise_patch) for _ in range(4)],
            ],
        )
        settings = _settings(tmp_path / "revise.db")
        settings = settings.model_copy(
            update={
                "scheduler": settings.scheduler.model_copy(update={"max_revisions": 3}),
            }
        )
        async with sdk_hub(tmp_path, "revise.db", settings=settings, llm=llm) as (
            app,
            http,
            client,
        ):
            await http.post("/v1/agents", json={"name": "writer", "card_url": writer.url})
            task_id = await _send_once(client, _message("写一份材料"))
            task = await wait_for_task(
                client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30
            )
            state = task_state(task)
            assert state["revision_count"] == 3
    finally:
        await writer.stop()
