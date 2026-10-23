from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from typing import cast, final, override

import pytest
from a2a.helpers.proto_helpers import new_message, new_text_part
from a2a.server.events import EventQueue
from a2a.types.a2a_pb2 import Role

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.models.llm_request import LLMRequest
from new_choirworks.core.models.llm_response import LLMResponse, LLMResponseContent, LLMResponsePart
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.errors import WorkflowError
from new_choirworks.orchestrator.catalog import AgentInfo
from new_choirworks.orchestrator.orchestrator import OrchestratorAgent, build_workflow
from new_choirworks.orchestrator.plan import DagPlan, PlanNode
from new_choirworks.orchestrator.runtime import DagRuntime

from ..conftest import FakeLLM

AGENTS = {
    "writer": AgentInfo(name="writer", url="http://writer"),
    "reviewer": AgentInfo(name="reviewer", url="http://reviewer"),
}


def _node(nid: str, deps: list[str] | None = None, agent: str = "writer") -> PlanNode:
    return PlanNode(
        id=nid,
        name=nid,
        agent_name=agent,
        instruction=f"instruction-{nid}",
        deps=deps or [],
    )


def _plan_json(*nodes: PlanNode) -> str:
    return json.dumps(DagPlan(nodes=list(nodes)).model_dump())


def _chunk(text: str) -> LLMResponse:
    return LLMResponse(content=LLMResponseContent(parts=[LLMResponsePart(text=text)]))


class _FakeQueue(EventQueue):
    def __init__(self) -> None:
        self.events: list[object] = []

    @override
    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


@final
class _ScriptRunner:
    """按 agent_name 队列出结果；记录派发顺序/消息/取消到共享 log。"""

    def __init__(
        self,
        outputs: dict[str, list[str]],
        log: list[str],
        delays: dict[str, float] | None = None,
        failures: set[str] | None = None,
    ) -> None:
        self.outputs = {k: list(v) for k, v in outputs.items()}
        self.log = log
        self.delays = delays or {}
        self.failures = failures or set()
        self.messages: list[tuple[str, str]] = []
        self.cancelled: list[str] = []

    async def __call__(self, ctx: Context, node: PlanNode, info: AgentInfo, message: str) -> str:
        self.log.append(f"run:{node.id}")
        self.messages.append((node.id, message))
        try:
            await asyncio.sleep(self.delays.get(node.id, 0.01))
        except asyncio.CancelledError:
            self.cancelled.append(node.id)
            raise
        if node.id in self.failures:
            raise RuntimeError(f"boom:{node.id}")
        queue = self.outputs.get(node.agent_name, [])
        return queue.pop(0) if queue else f"output-{node.id}"


def _llm(plans: list[str], log: list[str]) -> FakeLLM:
    def responder(i: int, _req: LLMRequest) -> list[LLMResponse]:
        log.append(f"llm:{i}")
        return [_chunk(plans[min(i, len(plans) - 1)])]

    return FakeLLM(responder=responder)


def _ic() -> InvocationContext:
    user_msg = new_message(parts=[new_text_part(text="do something")], role=Role.ROLE_USER)
    return InvocationContext(
        events=[user_msg],
        event_queue=_FakeQueue(),
        task_id="t1",
        context_id="c1",
    )


def _outputs(ctx: Context) -> dict[str, str]:
    return cast(dict[str, str], ctx.output)


async def _run_workflow(runtime: DagRuntime, llm: FakeLLM, runner: _ScriptRunner) -> Context:
    workflow = build_workflow(runtime, llm, node_runner=runner)
    ctx = Context(invocation_context=_ic(), node=workflow)
    agen: AsyncGenerator[A2AEvent, None] = workflow.run(ctx, node_input="do something")
    async for _ in agen:
        pass
    return ctx


async def test_parallel_per_completion_judgment_then_done():
    """① A/B 并行，逐个判定（每个完成各走一轮），空计划 = 完成。"""
    runtime = DagRuntime(agents=AGENTS, user_task="do something")
    log: list[str] = []
    runner = _ScriptRunner({"writer": ["plain a"], "reviewer": ["plain b"]}, log)
    llm = _llm(
        [
            _plan_json(_node("a"), _node("b", agent="reviewer")),
            _plan_json(_node("a"), _node("b", agent="reviewer")),
            _plan_json(),
        ],
        log,
    )

    ctx = await _run_workflow(runtime, llm, runner)

    assert sorted(n for n, _ in runner.messages) == ["a", "b"]
    assert len([x for x in log if x.startswith("llm:")]) == 3  # 初始规划 + 两次判定各一轮
    assert ctx.output == {"a": "plain a", "b": "plain b"}


async def test_assist_roundtrip_finds_output_back_to_parent():
    """② @mention → assist 节点 → B 完成 → output 经 deps 注入交还 A → A 续轮。"""
    runtime = DagRuntime(agents=AGENTS, user_task="do something")
    log: list[str] = []
    runner = _ScriptRunner(
        {"writer": ["@reviewer please review the draft", "final report"], "reviewer": ["LGTM"]},
        log,
    )
    llm = _llm([_plan_json(_node("a"))], log)

    ctx = await _run_workflow(runtime, llm, runner)

    # writer 两轮：求助轮 + 续轮；reviewer 一轮 assist
    assert [n for n, _ in runner.messages] == ["a", "a-assist1", "a"]
    round2 = runner.messages[2][1]
    assert "LGTM" in round2
    assert _outputs(ctx)["a"] == "final report"
    assert _outputs(ctx)["a-assist1"] == "LGTM"


async def test_replan_drops_running_node_but_keeps_result():
    """③ 重规划：A 不重跑、C 拿 A output；B 被移出计划但跑完、结果留存。"""
    runtime = DagRuntime(agents=AGENTS, user_task="do something")
    log: list[str] = []
    runner = _ScriptRunner(
        {"writer": ["out_a", "out_c"], "reviewer": ["out_b"]},
        log,
        delays={"b": 0.2},
    )
    llm = _llm(
        [
            _plan_json(_node("a"), _node("b", agent="reviewer")),
            _plan_json(_node("a"), _node("c", deps=["a"])),  # b 不再需要
            _plan_json(),
            _plan_json(),
        ],
        log,
    )

    ctx = await _run_workflow(runtime, llm, runner)

    assert [n for n, _ in runner.messages].count("a") == 1  # 已完成不重跑
    c_msg = next(m for n, m in runner.messages if n == "c")
    assert "out_a" in c_msg
    assert "out_b" not in c_msg
    assert _outputs(ctx)["b"] == "out_b"  # 结果留存（不引用则忽略）


async def test_judgment_pause_dispatches_after_evaluation():
    """④ 评估暂停：判定轮全部走完才派发新就绪节点（消费优先）。"""
    runtime = DagRuntime(agents=AGENTS, user_task="do something")
    log: list[str] = []
    runner = _ScriptRunner({"writer": ["pa", "pb", "pc"]}, log)
    full = _plan_json(_node("a"), _node("b"), _node("c", deps=["a", "b"]))
    llm = _llm([full, full, full], log)

    _ = await _run_workflow(runtime, llm, runner)

    # 初始规划 llm:0 之后是两轮判定 llm:1/llm:2，c 只能在其后派发
    assert log.index("run:c") > log.index("llm:2")


async def test_failed_node_fails_whole_run_and_cancels():
    """⑤ FAILED → 整体失败 + 本地在途任务取消。"""
    runtime = DagRuntime(agents=AGENTS, user_task="do something")
    log: list[str] = []
    runner = _ScriptRunner(
        {"writer": ["x"], "reviewer": ["y"]},
        log,
        delays={"bad": 0.01, "slow": 0.5},
        failures={"bad"},
    )
    llm = _llm([_plan_json(_node("bad"), _node("slow", agent="reviewer"))], log)

    with pytest.raises(WorkflowError, match="node bad failed: boom:bad"):
        _ = await _run_workflow(runtime, llm, runner)

    assert runner.cancelled == ["slow"]
    assert runtime.running_tasks == {}


async def test_empty_plan_means_complete():
    """⑥ Planner 输出空计划 = 完成。"""
    runtime = DagRuntime(agents=AGENTS, user_task="do something")
    log: list[str] = []
    runner = _ScriptRunner({"writer": ["plain"]}, log)
    llm = _llm([_plan_json(_node("a")), _plan_json()], log)

    ctx = await _run_workflow(runtime, llm, runner)

    assert ctx.output == {"a": "plain"}
    assert runtime.has_work() is False


async def test_orchestrator_agent_wiring():
    """OrchestratorAgent：组图 + 用户任务提取 + node_runner 注入生效。"""
    log: list[str] = []
    runner = _ScriptRunner({"writer": ["plain"]}, log)
    llm = _llm([_plan_json(_node("a")), _plan_json()], log)
    agent = OrchestratorAgent(
        name="orchestrator", agents=list(AGENTS.values()), llm=llm, node_runner=runner
    )

    ic = _ic()
    async for _event in agent.run_async(ic):
        pass

    assert [n for n, _ in runner.messages] == ["a"]
