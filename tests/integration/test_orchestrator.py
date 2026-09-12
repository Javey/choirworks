import asyncio

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.core.planner import PlanDraft, Planner, PlanNodeDraft
from agent_hub.core.tasks import TaskService
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.sim.fake_agent import start_fake_agent
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.support.fakes import FakeLLM


def draft(*nodes: PlanNodeDraft, rationale: str = "test") -> PlanDraft:
    return PlanDraft(rationale=rationale, nodes=list(nodes))


def n(node_id: str, agent: str = "good", deps: list[str] | None = None, text: str = "x"):
    return PlanNodeDraft(
        id=node_id, name=node_id, agent_name=agent, deps=deps or [], input={"text": text}
    )


async def setup(
    tmp_path,
    llm_results,
    agents,
    *,
    max_parallel=5,
    max_attempts=2,
    backoff=0.0,
    replan=True,
):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    for name, agent in agents.items():
        await registry.register(name, agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=10.0)
    llm = FakeLLM(structured_results=llm_results)
    planner = Planner(llm, registry)
    orchestrator = Orchestrator(
        db,
        events,
        planner,
        dispatcher,
        tasks,
        max_parallel=max_parallel,
        max_node_attempts=max_attempts,
        retry_backoff_seconds=backoff,
        replan_on_failure=replan,
    )
    return db, remote, events, tasks, orchestrator, llm


async def test_auto_single_node(tmp_path):
    agent = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path, [draft(n("n1", text="hi"))], {"good": agent}
    )
    try:
        task_id = await tasks.create_pending_task("hi")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].output["artifacts"][0]["text"] == "echo:hi"
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await agent.stop()


async def test_diamond_runs_parallel_nodes_concurrently(tmp_path):
    slow = await start_fake_agent("delay")
    fast = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path,
        [draft(n("a", text="1"), n("b", text="2"), n("c", deps=["a", "b"], text="join"))],
        {"good": slow, "fast": fast},
    )
    try:
        task_id = await tasks.create_pending_task("diamond")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED

        event_list = await events.replay(task_id)
        dispatched = [e for e in event_list if e.type is EventType.NODE_DISPATCHED]
        first_completed = next(
            e
            for e in event_list
            if e.type is EventType.NODE_STATE_CHANGED
            and e.payload.get("to") == "completed"
        )
        a_dispatch = next(e for e in dispatched if e.payload["node_id"].endswith(":a"))
        b_dispatch = next(e for e in dispatched if e.payload["node_id"].endswith(":b"))
        assert a_dispatch.seq < first_completed.seq
        assert b_dispatch.seq < first_completed.seq
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await slow.stop()
        await fast.stop()


async def test_retry_then_success(tmp_path):
    flaky = await start_fake_agent("fail_once")
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path, [draft(n("n1"))], {"good": flaky}, max_attempts=2
    )
    try:
        task_id = await tasks.create_pending_task("x")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].attempt == 2
        types = [e.type for e in await events.replay(task_id)]
        assert EventType.NODE_RETRY_SCHEDULED in types
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await flaky.stop()


async def test_replan_on_permanent_failure(tmp_path):
    bad = await start_fake_agent("fail")
    good = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator, llm = await setup(
        tmp_path,
        [draft(n("n1", agent="bad")), draft(n("n1", agent="good", text="retry"))],
        {"bad": bad, "good": good},
        max_attempts=1,
        replan=True,
    )
    try:
        task_id = await tasks.create_pending_task("x")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.plan is not None and snapshot.plan.version == 2
        types = [e.type for e in await events.replay(task_id)]
        assert types.count(EventType.PLAN_CREATED) == 2
        assert EventType.PLAN_SUPERSEDED in types
        assert len(llm.structured_calls) == 2
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await bad.stop()
        await good.stop()


async def test_planning_failure_marks_task_failed(tmp_path):
    good = await start_fake_agent("echo")
    bad_plan = draft(n("n1", agent="ghost"))
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path, [bad_plan, bad_plan, bad_plan], {"good": good}
    )
    try:
        task_id = await tasks.create_pending_task("x")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.FAILED
        assert EventType.ERROR in [e.type for e in await events.replay(task_id)]
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await good.stop()


async def test_input_required_parks_task(tmp_path):
    asker = await start_fake_agent("ask")
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path, [draft(n("n1", text="ask"))], {"good": asker}
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.AWAITING_INPUT
        assert snapshot.nodes[0].status is NodeStatus.INPUT_REQUIRED
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await asker.stop()


async def test_checkpoints_created_as_nodes_complete(tmp_path):
    agent = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator, _ = await setup(
        tmp_path, [draft(n("n1", text="hi"))], {"good": agent}
    )
    try:
        task_id = await tasks.create_pending_task("hi")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        from agent_hub.store import projections as proj

        checkpoints = await proj.fetch_checkpoints(db, task_id)
        assert len(checkpoints) == 1
        assert checkpoints[0].frontier
        assert checkpoints[0].plan_version == 1
        types = [e.type for e in await events.replay(task_id)]
        assert EventType.CHECKPOINT_CREATED in types
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await agent.stop()


async def test_initial_plan_receives_conversation_context(tmp_path):
    agent = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator, llm = await setup(
        tmp_path,
        [draft(n("n1", text="hi")), draft(n("n1", text="again"))],
        {"good": agent},
    )
    try:
        first = await tasks.create_pending_task("第一问")
        orchestrator.start(first)
        await asyncio.wait_for(orchestrator.wait(first, until_terminal=True), 10.0)
        conversation_id = (await tasks.get_snapshot(first)).task.conversation_id
        assert conversation_id

        second = await tasks.create_pending_task("追问", conversation_id=conversation_id)
        orchestrator.start(second)
        await asyncio.wait_for(orchestrator.wait(second, until_terminal=True), 10.0)

        first_prompt = llm.structured_calls[0]["user"]
        second_prompt = llm.structured_calls[1]["user"]
        assert "Completed work so far" not in first_prompt
        assert "User: 第一问" in second_prompt
        assert "Result: echo:hi" in second_prompt
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await agent.stop()
