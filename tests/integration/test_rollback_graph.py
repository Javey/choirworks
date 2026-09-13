
from choirworks.a2a.client import RemoteAgentClient
from choirworks.a2a.registry import AgentRegistry
from choirworks.config import PolicyConfig
from choirworks.core.dispatcher import NodeDispatcher
from choirworks.core.orchestrator import Orchestrator, PeerChoice
from choirworks.core.planner import PlanDraft, Planner, PlanNodeDraft
from choirworks.core.policy import PolicyEngine
from choirworks.core.rollback import perform_rollback, plan_rollback
from choirworks.core.tasks import TaskService
from choirworks.models.enums import EventType, NodeStatus, TaskStatus
from choirworks.sim.fake_agent import start_fake_agent
from choirworks.store import projections
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore
from tests.support.fakes import FakeLLM


def plan() -> PlanDraft:
    return PlanDraft(
        rationale="two nodes",
        nodes=[
            PlanNodeDraft(id="n1", name="echoer", agent_name="echoer", input={"text": "hi"}),
            PlanNodeDraft(
                id="n2", name="asker", agent_name="asker", input={"text": "help?"}
            ),
        ],
    )


async def setup(tmp_path, echoer, asker, helper):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    await registry.register("echoer", echoer.url)
    await registry.register("asker", asker.url)
    await registry.register("helper", helper.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=10.0)
    llm = FakeLLM(
        structured_results=[
            plan(),
            PeerChoice(agent_name="helper", instruction="请补充信息：helper 答复"),
            PeerChoice(agent_name="helper", instruction="请补充信息：helper 答复"),
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
        max_node_attempts=2,
        retry_backoff_seconds=0.0,
        replan_on_failure=False,
    )
    return db, remote, events, tasks, orchestrator


async def test_rollback_invalidates_extension_and_restores_graph(tmp_path):
    echoer = await start_fake_agent("echo")
    asker = await start_fake_agent("ask")
    helper = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator = await setup(tmp_path, echoer, asker, helper)
    try:
        task_id = await tasks.create_pending_task("do things")
        orchestrator.start(task_id)
        await orchestrator.wait(task_id, until_terminal=True)

        checkpoints = await projections.fetch_checkpoints(db, task_id)
        assert len(checkpoints) >= 2
        before_extension = checkpoints[0]
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.plan is not None
        old_helper_id = next(
            node.id for node in snapshot.nodes if node.agent_name == "helper"
        )

        report = await plan_rollback(
            db, events, task_id, before_extension.id
        )
        assert old_helper_id in report.invalidated_node_ids
        assert any(node_id.endswith(":n2") for node_id in report.reset_node_ids)

        await perform_rollback(db, events, remote, orchestrator, task_id, before_extension.id)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.RUNNING
        assert snapshot.plan is not None
        dag_nodes = {node["id"]: node for node in snapshot.plan.dag["nodes"]}
        assert set(dag_nodes) == {"n1", "n2"}
        assert dag_nodes["n2"]["deps"] == []

        old_helper = await projections.fetch_node(db, old_helper_id)
        assert old_helper is not None
        assert old_helper.status is NodeStatus.INVALIDATED
        parent = next(node for node in snapshot.nodes if node.id.endswith(":n2"))
        assert parent.deps == []
        assert parent.status is NodeStatus.PENDING
        completed = next(node for node in snapshot.nodes if node.id.endswith(":n1"))
        assert completed.status is NodeStatus.COMPLETED

        await orchestrator.wait(task_id, until_terminal=True)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        if snapshot.plan is not None:
            new_helper_id = next(
                (
                    node.id
                    for node in snapshot.nodes
                    if node.agent_name == "helper" and node.status is NodeStatus.COMPLETED
                ),
                None,
            )
            assert new_helper_id is not None
            assert new_helper_id != old_helper_id

        replayed = await events.replay(task_id)
        assert sum(
            1 for event in replayed if event.type is EventType.PLAN_EXTENDED
        ) == 2

        await projections.rebuild(db)
        rebuilt = await tasks.get_snapshot(task_id)
        rebuilt_helper = await projections.fetch_node(db, old_helper_id)
        assert rebuilt_helper is not None
        assert rebuilt_helper.status is NodeStatus.INVALIDATED
        if rebuilt.plan is not None:
            rebuilt_ids = {node["id"] for node in rebuilt.plan.dag["nodes"]}
            assert {"n1", "n2"} <= rebuilt_ids
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await echoer.stop()
        await asker.stop()
        await helper.stop()


async def test_rollback_after_extension_reuses_completed_helper(tmp_path):
    echoer = await start_fake_agent("echo")
    asker = await start_fake_agent("ask")
    helper = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator = await setup(tmp_path, echoer, asker, helper)
    try:
        task_id = await tasks.create_pending_task("do things")
        orchestrator.start(task_id)
        await orchestrator.wait(task_id, until_terminal=True)

        checkpoints = await projections.fetch_checkpoints(db, task_id)
        assert len(checkpoints) >= 3
        after_helper = checkpoints[1]
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.plan is not None
        helper_id = next(
            node.id
            for node in snapshot.nodes
            if node.agent_name == "helper" and node.status is NodeStatus.COMPLETED
        )

        report = await plan_rollback(db, events, task_id, after_helper.id)
        assert helper_id not in report.invalidated_node_ids
        assert report.invalidated_node_ids == []

        await perform_rollback(
            db, events, remote, orchestrator, task_id, after_helper.id
        )
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.plan is not None
        dag_nodes = {node["id"]: node for node in snapshot.plan.dag["nodes"]}
        assert helper_id.split(":", 1)[1] in {
            key for key, node in dag_nodes.items() if node.get("derived")
        }
        parent = next(node for node in snapshot.nodes if node.id.endswith(":n2"))
        assert parent.status is NodeStatus.PENDING
        assert parent.deps == [helper_id]

        await orchestrator.wait(task_id, until_terminal=True)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        replayed = await events.replay(task_id)
        assert sum(
            1 for event in replayed if event.type is EventType.PLAN_EXTENDED
        ) == 1
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await echoer.stop()
        await asker.stop()
        await helper.stop()
