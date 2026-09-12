import asyncio

from agent_hub.a2a.client import RemoteAgentClient
from agent_hub.a2a.registry import AgentRegistry
from agent_hub.config import PolicyConfig, PolicyOverride
from agent_hub.core.dispatcher import NodeDispatcher
from agent_hub.core.orchestrator import Orchestrator, PeerChoice
from agent_hub.core.planner import PlanDraft, Planner, PlanNodeDraft
from agent_hub.core.policy import PolicyEngine
from agent_hub.core.tasks import TaskService
from agent_hub.models.enums import (
    EventType,
    InterventionStatus,
    NodeStatus,
    TaskStatus,
)
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore
from tests.fake_agents.echo_agent import start_fake_agent
from tests.support.fakes import FakeLLM


def plan_ask() -> PlanDraft:
    return PlanDraft(
        rationale="ask",
        nodes=[
            PlanNodeDraft(id="n1", name="n1", agent_name="asker", input={"text": "ask"})
        ],
    )


async def setup_hitl(tmp_path, agents, policies, structured=None, text=None):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    remote = RemoteAgentClient()
    registry = AgentRegistry(db, remote)
    for name, agent in agents.items():
        await registry.register(name, agent.url)
    events = EventStore(db)
    tasks = TaskService(db, events, registry)
    dispatcher = NodeDispatcher(db, events, remote, timeout_seconds=10.0)
    llm = FakeLLM(structured_results=structured, text_results=text)
    orchestrator = Orchestrator(
        db,
        events,
        Planner(llm, registry),
        dispatcher,
        tasks,
        registry=registry,
        remote=remote,
        llm=llm,
        policy_engine=PolicyEngine(policies),
        retry_backoff_seconds=0.0,
    )
    return db, remote, events, tasks, orchestrator, llm


async def test_auto_llm_answers_input_required(tmp_path):
    asker = await start_fake_agent("ask")
    db, remote, events, tasks, orchestrator, _ = await setup_hitl(
        tmp_path,
        {"asker": asker},
        PolicyConfig(default="auto_llm"),
        structured=[plan_ask()],
        text=["Bob"],
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].output["artifacts"][0]["text"] == "answered:Bob"
        interventions = await projections.fetch_interventions(db, task_id)
        assert len(interventions) == 1
        assert interventions[0].status is InterventionStatus.RESOLVED
        assert interventions[0].responder == "auto_llm"
        types = [e.type for e in await events.replay(task_id)]
        assert EventType.INTERVENTION_REQUESTED in types
        assert EventType.INTERVENTION_RESOLVED in types
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await asker.stop()


async def test_human_intervention_via_answer(tmp_path):
    asker = await start_fake_agent("ask")
    db, remote, events, tasks, orchestrator, _ = await setup_hitl(
        tmp_path,
        {"asker": asker},
        PolicyConfig(default="human", timeout_seconds=30),
        structured=[plan_ask()],
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.AWAITING_INPUT
        pending = await projections.fetch_interventions(
            db, task_id, InterventionStatus.PENDING
        )
        assert len(pending) == 1

        await orchestrator.answer_intervention(pending[0].id, "Bob", responder="user")
        await asyncio.wait_for(
            orchestrator.wait(task_id, until_terminal=True), 10.0
        )
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].output["artifacts"][0]["text"] == "answered:Bob"
        resolved = await projections.fetch_intervention(db, pending[0].id)
        assert resolved is not None and resolved.responder == "user"
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await asker.stop()


async def test_peer_agent_answers(tmp_path):
    asker = await start_fake_agent("ask")
    helper = await start_fake_agent("echo")
    db, remote, events, tasks, orchestrator, _ = await setup_hitl(
        tmp_path,
        {"asker": asker, "helper": helper},
        PolicyConfig(default="peer_agent"),
        structured=[
            plan_ask(),
            PeerChoice(agent_name="helper", instruction="lookup name"),
        ],
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        text = snapshot.nodes[0].output["artifacts"][0]["text"]
        assert text.startswith("answered:")
        assert "echo:lookup name" in text
        interventions = await projections.fetch_interventions(db, task_id)
        assert interventions[0].responder == helper.url
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await asker.stop()
        await helper.stop()


async def test_timeout_auto_fallback(tmp_path):
    asker = await start_fake_agent("ask")
    db, remote, events, tasks, orchestrator, _ = await setup_hitl(
        tmp_path,
        {"asker": asker},
        PolicyConfig(default="human", on_timeout="auto", timeout_seconds=0.2),
        structured=[plan_ask()],
        text=["Bob"],
    )
    try:
        task_id = await tasks.create_pending_task("ask")
        orchestrator.start(task_id)
        await asyncio.wait_for(orchestrator.wait(task_id), 10.0)
        await asyncio.wait_for(
            orchestrator.wait(task_id, until_terminal=True), 10.0
        )
        snapshot = await tasks.get_snapshot(task_id)
        assert snapshot.task.status is TaskStatus.COMPLETED
        assert snapshot.nodes[0].output["artifacts"][0]["text"] == "answered:Bob"
        interventions = await projections.fetch_interventions(db, task_id)
        assert any(i.responder == "auto_llm" for i in interventions)
    finally:
        await orchestrator.stop()
        await remote.close()
        await db.close()
        await asker.stop()


async def test_policy_override_forces_human(tmp_path):
    asker = await start_fake_agent("ask")
    db, remote, events, tasks, orchestrator, _ = await setup_hitl(
        tmp_path,
        {"asker": asker},
        PolicyConfig(
            default="auto_llm",
            timeout_seconds=30,
            overrides=[PolicyOverride(agent_name="asker", policy="human")],
        ),
        structured=[plan_ask()],
        text=["Bob"],
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
