from __future__ import annotations

import asyncio

import pytest

from new_choirworks.core.workflow.errors import WorkflowInvariantError
from new_choirworks.core.workflow.utils.replay_sequence_barrier import (
    ReplaySequenceBarrier,
)


async def test_first_key_is_ready() -> None:
    barrier = ReplaySequenceBarrier(["a@1", "b@1"])
    await asyncio.wait_for(barrier.wait("a@1"), timeout=0.1)


async def test_advance_unblocks_next_key() -> None:
    barrier = ReplaySequenceBarrier(["a@1", "b@1"])
    waiter = asyncio.create_task(barrier.wait("b@1"))
    await asyncio.sleep(0)
    assert not waiter.done()

    barrier.check_and_advance("a@1")
    await asyncio.wait_for(waiter, timeout=0.1)
    assert barrier.current_index == 1


async def test_wrong_key_does_not_advance() -> None:
    barrier = ReplaySequenceBarrier(["a@1", "b@1"])
    barrier.check_and_advance("wrong@1")
    assert barrier.current_index == 0
    assert not barrier.events["b@1"].is_set()


async def test_unknown_key_wait_returns_immediately() -> None:
    barrier = ReplaySequenceBarrier(["a@1"])
    await asyncio.wait_for(barrier.wait("unknown@1"), timeout=0.1)


async def test_timeout_raises_workflow_invariant_error() -> None:
    barrier = ReplaySequenceBarrier(["a@1", "b@1"], timeout_sec=0.01)
    with pytest.raises(WorkflowInvariantError):
        await barrier.wait("b@1")


async def test_empty_sequence_never_blocks() -> None:
    barrier = ReplaySequenceBarrier([])
    await asyncio.wait_for(barrier.wait("anything"), timeout=0.1)
    barrier.check_and_advance("anything")
    assert barrier.current_index == 0
