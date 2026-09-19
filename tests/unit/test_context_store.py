from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

from choirworks.store.contexts import ContextStore
from choirworks.store.db import Database


@contextlib.asynccontextmanager
async def _store(tmp_path) -> AsyncIterator[ContextStore]:
    db = Database(tmp_path / "ctx.db")
    await db.initialize()
    try:
        yield ContextStore(db)
    finally:
        await db.close()


async def test_upsert_and_get_round_trip(tmp_path):
    async with _store(tmp_path) as store:
        assert await store.get("c1") is None

        await store.upsert_state("c1", '{"plan_version": 2}')
        record = await store.get("c1")
        assert record is not None
        assert record.state == '{"plan_version": 2}'

        await store.upsert_state("c1", '{"plan_version": 3}')
        record = await store.get("c1")
        assert record is not None
        assert record.state == '{"plan_version": 3}'


async def test_create_sets_title_and_keeps_state(tmp_path):
    async with _store(tmp_path) as store:
        await store.upsert_state("c1", '{"plan_version": 2}')
        await store.create("c1", title="新对话")

        record = await store.get("c1")
        assert record is not None
        assert record.title == "新对话"
        assert record.state == '{"plan_version": 2}'


async def test_list_orders_by_updated_time(tmp_path):
    async with _store(tmp_path) as store:
        await store.create("c1", title="a")
        await store.create("c2", title="b")
        await store.upsert_state("c1", "{}")

        records = await store.list()
        assert [record.context_id for record in records] == ["c2", "c1"]


async def test_create_does_not_overwrite_existing_title(tmp_path):
    async with _store(tmp_path) as store:
        await store.create("c1", title="原来的")
        await store.create("c1", title="新的")

        record = await store.get("c1")
        assert record is not None
        assert record.title == "原来的"


async def test_rewind_restores_state_and_appends_marker(tmp_path):
    import json

    async with _store(tmp_path) as store:
        await store.create("c1", title="t")
        await store.rewind(
            "c1",
            state='{"plan_version": 2}',
            before_task_id="t2",
            cut_task_id="t3",
        )
        await store.rewind(
            "c1",
            state='{"plan_version": 1}',
            before_task_id="t4",
            cut_task_id="t4",
        )

        record = await store.get("c1")
        assert record is not None
        assert record.state == '{"plan_version": 1}'
        markers = json.loads(record.rewind_markers)
        assert [marker["before_task_id"] for marker in markers] == ["t2", "t4"]
        assert markers[0]["cut_task_id"] == "t3"
        assert markers[0]["created_at"]
