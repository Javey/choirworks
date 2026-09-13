import asyncio

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator
from agent_hub.core.planner import PlanDraft, Planner, PlanNodeDraft
from agent_hub.core.rollback import perform_rollback, plan_rollback
from agent_hub.core.tasks import TargetSpec, TaskService
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.sim.fake_agent import start_fake_agent
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.support.fakes import FakeLLM


async def setup_env(tmp_path, agent, *, replan=True, max_attempts=2):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
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
        max_node_attempts=max_attempts,
        replan_on_failure=replan,
        retry_backoff_seconds=0.0,
    )
    return db, remote, events, tasks, dispatcher, orchestrator


async def test_rollback_dry_run_and_restart(tmp_path):
    agent = await start_fake_agent("echo")
    db, remote, events, tasks, dispatcher, orchestrator = await setup_env(tmp_path, agent)
    try:
        created = await tasks.create_task("hi", TargetSpec(agent_name="fake"))
        orchestrator.start(created.task_id)
        await asyncio.wait_for(
            orchestrator.wait(created.task_id, until_terminal=True), 10.0
        )
        checkpoints = await projections.fetch_checkpoints(db, created.task_id)
        assert checkpoints
        checkpoint = checkpoints[0]

        second = PlanDraft(
            rationale="v2",
            nodes=[
                PlanNodeDraft(
                    id="n2",
                    name="n2",
                    agent_name="fake",
                    deps=["n1"],
                    input={"text": "again"},
                )
            ],
        )
        await tasks.create_plan_from_draft(created.task_id, second, version=2)
        plan2 = await projections.fetch_current_plan(db, created.task_id)
        assert plan2 is not None and plan2.version == 2
        node2_id = f"{plan2.id}:n2"

        dry = await plan_rollback(db, events, created.task_id, checkpoint.id)
        assert dry.mode == "dry_run"
        assert node2_id in dry.invalidated_node_ids
        assert EventType.ROLLBACK_PERFORMED not in [
            e.type for e in await events.replay(created.task_id)
        ]

        report = await perform_rollback(
            db, events, remote, orchestrator, created.task_id, checkpoint.id
        )
        assert report.mode == "restart"
        await asyncio.wait_for(
            orchestrator.wait(created.task_id, until_terminal=True), 10.0
        )
        task = await projections.fetch_task(db, created.task_id)
        assert task is not None and task.status is TaskStatus.COMPLETED
        assert task.plan_version == checkpoint.plan_version
        node2 = await projections.fetch_node(db, node2_id)
        assert node2 is not None and node2.status is NodeStatus.INVALIDATED
        types = [e.type for e in await events.replay(created.task_id)]
        assert EventType.ROLLBACK_PERFORMED in types
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await agent.stop()


async def test_retry_failed_node_then_success(tmp_path):
    from agent_hub.sim.fake_agent import start_fake_agent as start

    flaky = await start("fail_once")
    db, remote, events, tasks, dispatcher, orchestrator = await setup_env(
        tmp_path, flaky, replan=False, max_attempts=1
    )
    try:
        draft = PlanDraft(
            rationale="one",
            nodes=[
                PlanNodeDraft(id="n1", name="n1", agent_name="fake", input={"text": "x"})
            ],
        )
        task_id = await tasks.create_pending_task("x")
        orchestrator._planner._llm.structured_results.append(draft)  # noqa: SLF001
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id, until_terminal=True), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.FAILED
        node_id = snapshot.nodes[0].id

        retried = await tasks.retry_node(task_id, node_id)
        assert retried.status is NodeStatus.READY
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id, until_terminal=True), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].attempt == 2
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await flaky.stop()
