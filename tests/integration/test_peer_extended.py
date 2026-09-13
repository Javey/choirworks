import asyncio

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.config import PolicyConfig
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator, PeerChoice
from agent_hub.core.planner import Planner
from agent_hub.core.policy import PolicyEngine
from agent_hub.core.tasks import TargetSpec, TaskService
from agent_hub.models.enums import EventType, InterventionStatus, NodeStatus, TaskStatus
from agent_hub.sim.fake_agent import start_fake_agent
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.support.fakes import FakeLLM


async def setup(tmp_path, worker, helper, *, max_attempts=2, replan=False):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("worker", worker.url)
    await registry.register("helper", helper.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=10.0)
    llm = FakeLLM(
        structured_results=[
            PeerChoice(agent_name="helper", instruction="请补充信息：这是 C 的答复")
        ]
    )
    orchestrator = Orchestrator(
        db,
        events,
        Planner(llm, registry),
        dispatcher,
        tasks,
        registry=registry,
        remote=remote,
        llm=llm,
        policy_engine=PolicyEngine(PolicyConfig(default="peer_agent")),
        max_node_attempts=max_attempts,
        retry_backoff_seconds=0.0,
        replan_on_failure=replan,
    )
    return db, remote, events, tasks, orchestrator


async def wait_for(predicate, timeout_seconds=15.0):
    deadline = asyncio.get_event_loop().time() + timeout_seconds
    while asyncio.get_event_loop().time() < deadline:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met in time")


async def test_peer_creates_extension_and_resumes_parent(tmp_path):
    worker = await start_fake_agent("ask")
    helper = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator = await setup(tmp_path, worker, helper)
    try:
        created = await tasks.create_task("需要协助", TargetSpec(agent_name="worker"))
        orchestrator.start(created.task_id)
        await orchestrator.wait(created.task_id, until_terminal=True)

        snapshot = await tasks.get_snapshot(created.task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.plan is not None
        dag_nodes = {node["id"]: node for node in snapshot.plan.dag["nodes"]}
        assert set(dag_nodes) == {"n1", "a1"} or len(dag_nodes) == 2
        helper_key = next(key for key in dag_nodes if key != "n1")
        assert dag_nodes[helper_key]["derived"] is True
        assert dag_nodes["n1"]["deps"] == [helper_key]

        parent = next(node for node in snapshot.nodes if node.id.endswith(":n1"))
        helper_node = next(
            node for node in snapshot.nodes if not node.id.endswith(":n1")
        )
        assert parent.status is NodeStatus.COMPLETED
        assert parent.deps == [helper_node.id]
        assert helper_node.status is NodeStatus.COMPLETED
        assert helper_node.output is not None
        assert "echo:请补充信息" in helper_node.output["artifacts"][0]["text"]

        replayed = await events.replay(created.task_id)
        types = [event.type for event in replayed]
        assert EventType.PLAN_EXTENDED in types
        assert EventType.INTERVENTION_REQUESTED in types
        assert EventType.INTERVENTION_RESOLVED in types

        requested = next(
            event for event in replayed if event.type is EventType.INTERVENTION_REQUESTED
        )
        assert requested.payload["assigned_node_id"] == helper_node.id
        assert requested.payload["assigned_to"] == "helper"

        resolved = next(
            event for event in replayed if event.type is EventType.INTERVENTION_RESOLVED
        )
        assert resolved.payload["responder"] == "helper"
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await worker.stop()
        await helper.stop()


async def test_parent_stays_running_while_helper_works(tmp_path):
    worker = await start_fake_agent("ask")
    helper = await start_fake_agent("delay")
    db, remote, events, tasks, orchestrator = await setup(tmp_path, worker, helper)
    try:
        created = await tasks.create_task("需要协助", TargetSpec(agent_name="worker"))
        orchestrator.start(created.task_id)

        statuses: list[TaskStatus] = []

        async def sample() -> bool:
            task = await projections.fetch_task(db, created.task_id)
            assert task is not None
            statuses.append(task.status)
            return task.status is TaskStatus.COMPLETED

        await wait_for(sample)
        assert TaskStatus.AWAITING_INPUT not in statuses
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await worker.stop()
        await helper.stop()


async def test_helper_failure_marks_intervention_failed(tmp_path):
    worker = await start_fake_agent("ask")
    helper = await start_fake_agent("fail")
    db, remote, events, tasks, orchestrator = await setup(
        tmp_path, worker, helper, max_attempts=1, replan=False
    )
    try:
        created = await tasks.create_task("需要协助", TargetSpec(agent_name="worker"))
        orchestrator.start(created.task_id)
        await orchestrator.wait(created.task_id, until_terminal=True)

        snapshot = await tasks.get_snapshot(created.task_id)
        assert snapshot.task.status is TaskStatus.FAILED
        interventions = await projections.fetch_interventions(db, created.task_id)
        assert len(interventions) == 1
        assert interventions[0].status is InterventionStatus.FAILED
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await worker.stop()
        await helper.stop()
