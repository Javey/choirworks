from datetime import UTC, datetime

from agent_hub.models.enums import EventType, InterventionStatus
from agent_hub.store import projections
from agent_hub.store.db import Database
from agent_hub.store.event_store import EventStore


async def test_intervention_lifecycle_projection(tmp_path):
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
                "responder": None,
                "deadline_at": datetime.now(UTC).isoformat(),
            },
        )
        pending = await projections.fetch_interventions(
            db, "t1", InterventionStatus.PENDING
        )
        assert len(pending) == 1
        assert pending[0].question == {"text": "who?"}
        assert pending[0].deadline_at is not None

        await store.append(
            "t1",
            EventType.INTERVENTION_RESOLVED,
            {
                "intervention_id": "iv1",
                "answer": {"text": "Bob"},
                "responder": "user",
            },
        )
        resolved = await projections.fetch_intervention(db, "iv1")
        assert resolved is not None
        assert resolved.status is InterventionStatus.RESOLVED
        assert resolved.answer == {"text": "Bob"}
        assert resolved.responder == "user"
        assert resolved.resolved_at is not None
    finally:
        await db.close()


async def test_nodes_store_agent_name_and_policy_override(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    try:
        store = EventStore(db)
        await store.append("t1", EventType.TASK_CREATED, {"request": "x", "policy": None})
        await store.append(
            "t1",
            EventType.PLAN_CREATED,
            {
                "plan_id": "p1",
                "version": 1,
                "rationale": "x",
                "dag": {
                    "nodes": [
                        {
                            "id": "n1",
                            "name": "step",
                            "agent_url": "http://a",
                            "agent_name": "helper",
                            "skill_id": "s",
                            "deps": [],
                            "input": {},
                            "requires_approval": False,
                            "policy_override": "human",
                        }
                    ]
                },
            },
        )
        node = await projections.fetch_node(db, "p1:n1")
        assert node is not None
        assert node.agent_name == "helper"
        assert node.policy_override == "human"
    finally:
        await db.close()
