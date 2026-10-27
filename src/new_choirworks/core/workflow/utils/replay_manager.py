# 设计参考 google-adk workflow/utils/_replay_manager.py
# （Apache-2.0, Copyright 2026 Google LLC）
from __future__ import annotations

from new_choirworks.core.events.event import A2AEvent, meta
from new_choirworks.core.events.node_path_builder import NodePathBuilder
from new_choirworks.core.workflow.utils.rehydration_utils import (
    ChildScanState,
    is_terminal_event,
    reconstruct_node_states,
)
from new_choirworks.core.workflow.utils.replay_sequence_barrier import (
    ReplaySequenceBarrier,
)


class ReplayManager:
    """从事件流生成 recovered node states 与重放序列屏障。

    - ``scan_workflow_events``：workflow 级，按直接子节点分组恢复状态，
      并建立宽松（transitive）的 sequence barrier；
    - ``prepare_parent_sequence_barrier``：动态父节点级，只用直接子节点
      事件建立严格 sequence barrier。
    """

    _sequence_barrier: ReplaySequenceBarrier | None
    _parent_sequence_barriers: dict[str, ReplaySequenceBarrier]

    def __init__(self) -> None:
        self._sequence_barrier = None
        self._parent_sequence_barriers = {}

    @property
    def sequence_barrier(self) -> ReplaySequenceBarrier | None:
        return self._sequence_barrier

    def scan_workflow_events(
        self, events: list[A2AEvent], base_path: str
    ) -> tuple[dict[str, ChildScanState], list[str]]:
        states = reconstruct_node_states(events, base_path, group_by_direct_child=True)
        sequence = self._scan_sequence(events, base_path, strict_direct_child=False)
        self._sequence_barrier = ReplaySequenceBarrier(sequence)
        self._parent_sequence_barriers[base_path] = self._sequence_barrier
        return states, sequence

    def prepare_parent_sequence_barrier(
        self, events: list[A2AEvent], parent_path: str
    ) -> ReplaySequenceBarrier:
        barrier = self._parent_sequence_barriers.get(parent_path)
        if barrier is None:
            sequence = self._scan_sequence(events, parent_path, strict_direct_child=True)
            barrier = ReplaySequenceBarrier(sequence)
            self._parent_sequence_barriers[parent_path] = barrier
        return barrier

    async def advance_sequence(self, parent_path: str, key: str) -> None:
        barrier = self._parent_sequence_barriers.get(parent_path)
        if barrier is not None:
            barrier.check_and_advance(key)

    async def wait_sequence(self, parent_path: str, key: str) -> None:
        barrier = self._parent_sequence_barriers.get(parent_path)
        if barrier is not None:
            await barrier.wait(key)

    def _scan_sequence(
        self,
        events: list[A2AEvent],
        base_path: str,
        *,
        strict_direct_child: bool = False,
    ) -> list[str]:
        base = NodePathBuilder.from_string(base_path)
        sequence: list[str] = []
        completed: set[str] = set()

        for event in events:
            path = NodePathBuilder.from_string(meta(event).node_info.path)
            if not path.is_descendant_of(base):
                continue

            child_path = base.get_direct_child(path)
            if strict_direct_child and path != child_path:
                continue

            output_for = meta(event).node_info.output_for
            is_child_event = path == child_path or (
                bool(output_for) and str(child_path) in output_for
            )
            if not is_child_event:
                continue

            if not is_terminal_event(event):
                continue

            segment = child_path.leaf_segment
            if segment in completed:
                continue
            if segment in sequence:
                sequence.remove(segment)
            sequence.append(segment)
            completed.add(segment)

        return sequence
