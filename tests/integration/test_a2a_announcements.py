from a2a.types import Message, Part, Role, SendMessageRequest, TaskState
from google.protobuf.json_format import MessageToDict

from choirworks.a2a.executor import OutcomeDecision
from choirworks.config import Settings
from choirworks.core.planner import PlanDraft, PlanNodeDraft
from tests.support.fakes import FakeLLM
from tests.support.sdk import sdk_hub, wait_for_task


def _node(node_id: str, name: str, agent_name: str, deps: list[str] | None = None) -> PlanNodeDraft:
    return PlanNodeDraft(
        id=node_id,
        name=name,
        agent_name=agent_name,
        input={"text": name},
        deps=list(deps or []),
    )


def _message(text: str) -> SendMessageRequest:
    return SendMessageRequest(
        message=Message(message_id=f"m-{text}", role=Role.ROLE_USER, parts=[Part(text=text)])
    )


def _settings(tmp_path) -> Settings:
    return Settings(
        store={"db_path": tmp_path / "announce.db"},
        a2a={"public_url": "http://test"},
        scheduler={"retry_backoff_seconds": 0.0},
    )


def _function_calls(task) -> list[dict]:
    calls: list[dict] = []
    for artifact in task.artifacts:
        for part in artifact.parts:
            meta = part.metadata.fields
            if meta.get("cw_type") and meta["cw_type"].string_value == "function_call":
                calls.append(MessageToDict(part.data))
    return calls


def _subagent_calls(task) -> list[dict]:
    return [c for c in _function_calls(task) if c["function_name"] == "call_subagent"]


async def _send_and_wait(client, text: str, tmp_path):
    task_id = ""
    async for response in client.send_message(_message(text)):
        if response.WhichOneof("payload") == "task":
            task_id = response.task.id
    return await wait_for_task(
        client, task_id, {TaskState.TASK_STATE_COMPLETED}, timeout_seconds=30
    )


async def _register(http, name: str, url: str) -> None:
    resp = await http.post("/v1/agents", json={"name": name, "card_url": url})
    assert resp.status_code == 201, resp.text


async def test_serial_plan_announces_each_wave_in_order(tmp_path, echo_agent):
    llm = FakeLLM(
        structured_results=[
            PlanDraft(nodes=[_node("n1", "第一步", "echo"), _node("n2", "第二步", "echo", ["n1"])]),
            OutcomeDecision(intent="deliver"),
            OutcomeDecision(intent="deliver"),
        ]
    )
    async with sdk_hub(tmp_path, "announce.db", settings=_settings(tmp_path), llm=llm) as (
        app,
        http,
        client,
    ):
        await _register(http, "echo", echo_agent.url)
        task = await _send_and_wait(client, "串联任务", tmp_path)

        calls = _subagent_calls(task)
        assert [c["function_args"]["requested_by"] for c in calls] == [
            "orchestrator",
            "orchestrator",
        ]
        assert [c["function_args"]["target_agent"] for c in calls] == ["echo", "echo"]
        assert [c["function_args"]["instruction"] for c in calls] == ["第一步", "第二步"]

        replay = await http.get(f"/v1/conversations/{task.context_id}/replay")
        assert replay.status_code == 200, replay.text
        names = [
            part["data"]["function_name"]
            for event in replay.json()
            if "artifactUpdate" in event
            for part in event["artifactUpdate"]["artifact"].get("parts", [])
            if "data" in part
        ]
        assert names.count("call_subagent") == 2


async def test_parallel_plan_announces_one_wave(tmp_path, echo_agent):
    llm = FakeLLM(
        structured_results=[
            PlanDraft(nodes=[_node("n1", "并行甲", "echo"), _node("n2", "并行乙", "echo")]),
            OutcomeDecision(intent="deliver"),
            OutcomeDecision(intent="deliver"),
        ]
    )
    async with sdk_hub(tmp_path, "announce.db", settings=_settings(tmp_path), llm=llm) as (
        _app,
        http,
        client,
    ):
        await _register(http, "echo", echo_agent.url)
        task = await _send_and_wait(client, "并行任务", tmp_path)

        calls = _subagent_calls(task)
        assert [c["function_args"]["instruction"] for c in calls] == ["并行甲", "并行乙"]


async def test_retry_does_not_reannounce(tmp_path):
    from choirworks.sim.fake_agent import start_fake_agent

    flaky = await start_fake_agent("fail_once", name="flaky")
    try:
        llm = FakeLLM(
            structured_results=[
                PlanDraft(nodes=[_node("n1", "重试任务", "flaky")]),
                OutcomeDecision(intent="deliver"),
            ]
        )
        async with sdk_hub(
            tmp_path, "announce.db", settings=_settings(tmp_path), llm=llm
        ) as (_app, http, client):
            await _register(http, "flaky", flaky.url)
            task = await _send_and_wait(client, "重试任务", tmp_path)

            calls = _subagent_calls(task)
            assert [c["function_args"]["instruction"] for c in calls] == ["重试任务"]
            assert len(calls) == 1
    finally:
        await flaky.stop()


async def test_llm_requested_assist_announces_requester(tmp_path):
    from choirworks.sim.fake_agent import start_fake_agent

    ask = await start_fake_agent("ask", name="ask")
    echo = await start_fake_agent("echo", name="echo")
    try:
        llm = FakeLLM(
            structured_results=[
                PlanDraft(nodes=[_node("n1", "评估方案", "ask")]),
                OutcomeDecision(
                    intent="need_info", target_agent="echo", instruction="评估技术方案"
                ),
                OutcomeDecision(intent="deliver"),
                OutcomeDecision(intent="deliver"),
            ]
        )
        async with sdk_hub(tmp_path, "announce.db", settings=_settings(tmp_path), llm=llm) as (
            _app,
            http,
            client,
        ):
            await _register(http, "ask", ask.url)
            await _register(http, "echo", echo.url)
            task = await _send_and_wait(client, "请评估", tmp_path)

            calls = _subagent_calls(task)
            assert len(calls) == 2
            dispatch, assist = calls
            assert dispatch["function_args"]["requested_by"] == "orchestrator"
            assert dispatch["function_args"]["instruction"] == "评估方案"
            assert assist["function_args"]["requested_by"] == "n1"
            assert assist["function_args"]["target_agent"] == "echo"
            assert assist["function_args"]["instruction"] == "评估技术方案"
            assert assist["function_result"]["data"]["requester"] == "ask"
    finally:
        await ask.stop()
        await echo.stop()
