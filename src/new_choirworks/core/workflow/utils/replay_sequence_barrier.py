# 设计参考 google-adk workflow/utils/_replay_sequence_barrier.py
# （Apache-2.0, Copyright 2026 Google LLC）
from __future__ import annotations

import asyncio

from new_choirworks.core.workflow.errors import WorkflowInvariantError


class ReplaySequenceBarrier:
    """Deterministic replay ordering by chronological sequence.

    动态节点惰性重放时，按原始 ``node_path@run_id`` 序列推进：
    ``wait(key)`` 等待轮到该 key，``check_and_advance(key)`` 在当前期望项
    完成后放行下一项。
    """

    sequence: list[str]
    timeout_sec: float
    current_index: int
    events: dict[str, asyncio.Event]

    def __init__(self, sequence: list[str], timeout_sec: float = 15.0) -> None:
        self.sequence = sequence
        self.timeout_sec = timeout_sec
        self.current_index = 0
        self.events = {key: asyncio.Event() for key in sequence}
        if sequence:
            _ = self.events[sequence[0]].set()

    async def wait(self, key: str) -> None:
        """Block until ``key`` is unblocked.

        Keys not present in the sequence fast-forward: silent nodes that only
        produced state updates are not part of the barrier.
        """
        if key not in self.events:
            return
        try:
            _ = await asyncio.wait_for(self.events[key].wait(), timeout=self.timeout_sec)
        except TimeoutError as e:
            raise WorkflowInvariantError(
                "Replay divergence detected: Timed out waiting for sequence key"
                + f" '{key}' to be unblocked."
            ) from e

    def check_and_advance(self, key: str) -> None:
        """Advance the sequence if ``key`` matches the current expected key."""
        if self.current_index >= len(self.sequence):
            return
        expected_key = self.sequence[self.current_index]
        if key != expected_key:
            return
        self.current_index += 1
        if self.current_index < len(self.sequence):
            next_key = self.sequence[self.current_index]
            _ = self.events[next_key].set()
