import aiosqlite

from choirworks.models.enums import EventType, InterventionStatus
from choirworks.store import projections
from choirworks.store.db import Database
from choirworks.store.event_store import EventStore

N1 = {
    "id": "n1",
    "name": "writer",
    "agent_url": "http://writer",
    "agent_name": "writer",
    "deps": [],
    "input": {"text": "write"},
}


async def seed_plan(store: EventStore, task_id: str = "t1", plan_id: str = "p1") -> None:
    await store.append(
        task_id, EventType.TASK_CREATED, {"request": "x", "policy": None}
    )
    await store.append(
        task_id,
        EventType.PLAN_CREATED,
        {
            "plan_id": plan_id,
            "version": 1,
            "rationale": "seed",
            "dag": {"nodes": [dict(N1)]},
        },
    )


async def test_plan_extended_materializes_node_and_edge(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await seed_plan(store)
        await store.append(
            "t1",
            EventType.PLAN_EXTENDED,
            {
                "plan_id": "p1",
                "version": 1,
                "rationale": "peer assistance",
                "added_nodes": [
                    {
                        "id": "a1",
                        "name": "researcher",
                        "agent_url": "http://researcher",
                        "agent_name": "researcher",
                        "deps": [],
                        "input": {"text": "help"},
                        "derived": True,
                    }
                ],
                "added_edges": [{"from": "a1", "to": "n1"}],
            },
        )

        nodes = await projections.fetch_nodes(db, "t1", "p1")
        assert {node.id for node in nodes} == {"p1:n1", "p1:a1"}
        parent = next(node for node in nodes if node.id == "p1:n1")
        assert parent.deps == ["p1:a1"]
        helper = next(node for node in nodes if node.id == "p1:a1")
        assert helper.agent_name == "researcher"

        plan = await projections.fetch_current_plan(db, "t1")
        assert plan is not None
        dag_nodes = {node["id"]: node for node in plan.dag["nodes"]}
        assert set(dag_nodes) == {"n1", "a1"}
        assert dag_nodes["a1"]["derived"] is True
        assert dag_nodes["n1"]["deps"] == ["a1"]
    finally:
        await db.close()


async def test_plan_extended_rebuild_restores_graph(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await seed_plan(store)
        await store.append(
            "t1",
            EventType.PLAN_EXTENDED,
            {
                "plan_id": "p1",
                "version": 1,
                "rationale": "peer assistance",
                "added_nodes": [
                    {
                        "id": "a1",
                        "name": "researcher",
                        "agent_url": "http://researcher",
                        "agent_name": "researcher",
                        "deps": [],
                        "input": {"text": "help"},
                        "derived": True,
                    }
                ],
                "added_edges": [{"from": "a1", "to": "n1"}],
            },
        )
        await projections.rebuild(db)

        nodes = await projections.fetch_nodes(db, "t1", "p1")
        assert {node.id for node in nodes} == {"p1:n1", "p1:a1"}
        parent = next(node for node in nodes if node.id == "p1:n1")
        assert parent.deps == ["p1:a1"]
        plan = await projections.fetch_current_plan(db, "t1")
        assert plan is not None
        dag_nodes = {node["id"]: node for node in plan.dag["nodes"]}
        assert dag_nodes["a1"]["derived"] is True
        assert dag_nodes["n1"]["deps"] == ["a1"]
    finally:
        await db.close()


async def test_intervention_assignment_and_failure_projection(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await store.append(
            "t1",
            EventType.INTERVENTION_REQUESTED,
            {
                "intervention_id": "iv1",
                "node_id": "p1:n1",
                "source": "remote_input_required",
                "policy": "peer_agent",
                "question": {"text": "need help"},
                "assigned_node_id": "p1:a1",
                "assigned_to": "researcher",
            },
        )
        intervention = await projections.fetch_intervention(db, "iv1")
        assert intervention is not None
        assert intervention.assigned_node_id == "p1:a1"
        assert intervention.assigned_to == "researcher"
        assert intervention.status is InterventionStatus.PENDING

        await store.append(
            "t1", EventType.INTERVENTION_FAILED, {"intervention_id": "iv1"}
        )
        intervention = await projections.fetch_intervention(db, "iv1")
        assert intervention is not None
        assert intervention.status is InterventionStatus.FAILED
    finally:
        await db.close()


async def test_plan_superseded_invalidates_pending_interventions(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await store.append(
            "t1",
            EventType.INTERVENTION_REQUESTED,
            {
                "intervention_id": "iv1",
                "node_id": "p1:n1",
                "source": "remote_input_required",
                "policy": "human",
                "question": {"text": "who?"},
            },
        )
        await store.append(
            "t1",
            EventType.PLAN_SUPERSEDED,
            {"plan_id": "p1", "superseded_by_version": 2},
        )
        intervention = await projections.fetch_intervention(db, "iv1")
        assert intervention is not None
        assert intervention.status is InterventionStatus.INVALIDATED
    finally:
        await db.close()


OLD_INTERVENTIONS_SCHEMA = """
CREATE TABLE events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id    TEXT NOT NULL,
  type       TEXT NOT NULL,
  payload    TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE interventions (
  id          TEXT PRIMARY KEY,
  task_id     TEXT NOT NULL,
  node_id     TEXT,
  source      TEXT NOT NULL,
  policy      TEXT NOT NULL,
  question    TEXT NOT NULL,
  answer      TEXT,
  responder   TEXT,
  status      TEXT NOT NULL,
  deadline_at TEXT,
  created_at  TEXT NOT NULL,
  resolved_at TEXT
);
"""


async def test_migration_adds_assigned_node_id_to_old_db(tmp_path):
    path = tmp_path / "old.db"
    old = await aiosqlite.connect(path)
    await old.executescript(OLD_INTERVENTIONS_SCHEMA)
    await old.execute(
        "INSERT INTO interventions VALUES ('iv0', 't0', NULL, 's', 'human', '{}',"
        " NULL, NULL, 'pending', NULL, '2026-09-01T00:00:00+00:00', NULL)"
    )
    await old.commit()
    await old.close()

    db = Database(path)
    await db.initialize()
    try:
        store = EventStore(db)
        await store.append(
            "t1",
            EventType.INTERVENTION_REQUESTED,
            {
                "intervention_id": "iv1",
                "node_id": "p1:n1",
                "source": "remote_input_required",
                "policy": "peer_agent",
                "question": {"text": "help"},
                "assigned_node_id": "p1:a1",
            },
        )
        intervention = await projections.fetch_intervention(db, "iv1")
        assert intervention is not None
        assert intervention.assigned_node_id == "p1:a1"
        old_intervention = await projections.fetch_intervention(db, "iv0")
        assert old_intervention is not None
        assert old_intervention.assigned_node_id is None
    finally:
        await db.close()
