import pytest

from agent_hub.store.db import Database


async def test_initialize_creates_tables_and_wal(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()

    cursor = await db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    names = {row["name"] for row in await cursor.fetchall()}
    assert {
        "events",
        "orchestration_tasks",
        "plans",
        "nodes",
        "interventions",
        "checkpoints",
        "agent_registry",
    } <= names

    cursor = await db.conn.execute("PRAGMA journal_mode")
    mode = (await cursor.fetchone())[0]
    assert mode.lower() == "wal"
    await db.close()


async def test_transaction_rolls_back_on_error(tmp_path):
    db = Database(tmp_path / "hub.db")
    await db.initialize()
    with pytest.raises(RuntimeError):
        async with db.transaction() as conn:
            await conn.execute(
                "INSERT INTO agent_registry (id, name, card_url, card, created_at)"
                " VALUES ('a1', 'a', 'http://x', '{}', '2026-01-01T00:00:00+00:00')"
            )
            raise RuntimeError("boom")
    cursor = await db.conn.execute("SELECT COUNT(*) AS c FROM agent_registry")
    assert (await cursor.fetchone())["c"] == 0
    await db.close()
