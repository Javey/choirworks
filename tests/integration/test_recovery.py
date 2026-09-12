import asyncio
from pathlib import Path

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.reconcile import reconcile_once
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.core.planner import Planner
from agent_hub.core.recovery import recover_tasks
from agent_hub.core.tasks import TargetSpec, TaskService
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.sim.fake_agent import start_fake_agent
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.support.fakes import FakeLLM


async def _build(db_path: Path, agent):
    db = Database(db_path)
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    if await registry.get_by_name("fake") is None:
        await registry.register("fake", agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=10.0)
    llm = FakeLLM()
    orchestrator = Orchestrator(
        db,
        events,
        Planner(llm, registry),
        dispatcher,
        tasks,
        registry=registry,
        remote=remote,
        llm=llm,
    )
    return db, remote, events, tasks, dispatcher, orchestrator


async def test_recover_resumes_inflight_then_completes(tmp_path):
    agent = await start_fake_agent("delay")
    db_path = tmp_path / "hub.db"
    db, remote, events, tasks, dispatcher, orchestrator = await _build(db_path, agent)
    created = await tasks.create_task(
        "slow", TargetSpec(agent_name="fake", input={"text": "slow"})
    )
    orchestrator.start(created.task_id)
    for _ in range(200):
        node = await projections.fetch_node(db, created.node_ids[0])
        if node and node.status in (NodeStatus.DISPATCHED, NodeStatus.WORKING):
            break
        await asyncio.sleep(0.02)
    else:
        raise AssertionError("node never reached dispatched/working")
    await orchestrator.stop()
    await remote.close()
    await db.close()

    db2, remote2, events2, tasks2, dispatcher2, orchestrator2 = await _build(
        db_path, agent
    )
    try:
        result = await recover_tasks(
            db2, events2, remote2, dispatcher2, orchestrator2
        )
        assert created.task_id in result.recovered_task_ids
        await asyncio.wait_for(
            orchestrator2.wait(created.task_id, until_terminal=True), 10.0
        )
        if result.background:
            await asyncio.gather(*result.background, return_exceptions=True)
        snapshot = await tasks2.get_snapshot(created.task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].output is not None
    finally:
        await orchestrator2.stop()
        await remote2.close()
        await db2.close()
        await agent.stop()


async def test_recover_restarts_planning_task(tmp_path):
    agent = await start_fake_agent("echo")
    db_path = tmp_path / "hub.db"
    db, remote, events, tasks, dispatcher, orchestrator = await _build(db_path, agent)
    from agent_hub.models.enums import EventType as ET

    task_id = await tasks.create_pending_task("x")
    await orchestrator.stop()
    await remote.close()
    await db.close()

    db2, remote2, events2, tasks2, dispatcher2, orchestrator2 = await _build(
        db_path, agent
    )
    llm2 = orchestrator2._planner._llm  # noqa: SLF001 - 测试注入计划
    from agent_hub.core.planner import PlanDraft, PlanNodeDraft

    llm2.structured_results.append(
        PlanDraft(
            rationale="x",
            nodes=[
                PlanNodeDraft(
                    id="n1", name="n1", agent_name="fake", input={"text": "x"}
                )
            ],
        )
    )
    try:
        await recover_tasks(db2, events2, remote2, dispatcher2, orchestrator2)
        await asyncio.wait_for(orchestrator2.wait(task_id, until_terminal=True), 10.0)
        snapshot = await tasks2.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert ET.PLAN_CREATED in [e.type for e in await events2.replay(task_id)]
    finally:
        await orchestrator2.stop()
        await remote2.close()
        await db2.close()
        await agent.stop()


async def test_reconcile_detects_remote_completion(tmp_path):
    agent = await start_fake_agent("delay")
    db, remote, events, tasks, dispatcher, orchestrator = await _build(
        tmp_path / "hub.db", agent
    )
    try:
        created = await tasks.create_task(
            "slow", TargetSpec(agent_name="fake", input={"text": "hi"})
        )
        node_id = created.node_ids[0]
        stream = remote.send_text(agent.url, "hi", context_id=created.task_id)
        first = await anext(stream)
        await stream.aclose()
        await events.append(
            created.task_id,
            EventType.NODE_DISPATCHED,
            {
                "node_id": node_id,
                "a2a_task_id": first.task.id,
                "a2a_context_id": first.task.context_id,
                "message_id": "m1",
            },
        )
        await asyncio.sleep(0.8)
        fixed = await reconcile_once(db, events, remote)
        assert fixed == 1
        node = await projections.fetch_node(db, node_id)
        assert node is not None and node.status is NodeStatus.COMPLETED
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await agent.stop()
