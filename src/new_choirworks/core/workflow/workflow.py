# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_workflow.py
from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import override

from pydantic import Field

from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.base_node import START, BaseNode
from new_choirworks.core.workflow.context import Context
from new_choirworks.core.workflow.errors import WorkflowError
from new_choirworks.core.workflow.graph import Edge, Graph
from new_choirworks.core.workflow.node_runner import NodeRunner
from new_choirworks.core.workflow.node_state import NodeState
from new_choirworks.core.workflow.node_status import NodeStatus
from new_choirworks.core.workflow.trigger import Trigger
import asyncio

type TriggerBuffer = dict[str, list[Trigger]]
type PendingNodes = dict[str, asyncio.Task[Context]]


@dataclass
class _LoopState:
    """Mutable state for one Workflow execution."""

    trigger_buffer: TriggerBuffer = field(default_factory=dict)
    pending_tasks: PendingNodes = field(default_factory=dict)
    nodes: dict[str, NodeState] = field(default_factory=dict)
    node_outputs: dict[str, object] = field(default_factory=dict)


class Workflow(BaseNode):
    """A graph-based workflow node.

    Design reference: google-adk Workflow (Apache-2.0, Copyright 2026 Google LLC).
    Simplified: no trigger buffer replay, no retry/abort/interrupt/resume.

    _run_impl() is the graph orchestration loop:
    - SETUP: build graph, seed triggers
    - LOOP: schedule ready nodes, handle completions
    - FINALIZE: store node outputs
    """

    edges: list[Edge] = Field(default_factory=list)
    max_concurrency: int | None = None

    graph: Graph | None = None

    @override
    def model_post_init(self, context: object) -> None:
        super().model_post_init(context)
        self.graph = Graph(edges=self.edges)
        self.graph.validate_graph()

    def _get_static_node_by_name(self, name: str) -> BaseNode:
        """Find a node in the graph by name."""
        assert self.graph is not None
        for node in self.graph.nodes:
            if node.name == name:
                return node
        raise WorkflowError(f"node {name} not found in graph")

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        if self.graph is None:
            return

        loop_state = _LoopState()
        self._seed_start_triggers(loop_state, node_input)

        await self._run_loop(ctx, loop_state)

        return
        yield  # pyright: ignore[reportUnreachable]

    async def _run_loop(self, ctx: Context, loop_state: _LoopState) -> None:
        await self._schedule_ready_nodes(ctx, loop_state)
        
    async def _schedule_ready_nodes(self, ctx: Context, loop_state: _LoopState) -> None:
        for node_name in list(loop_state.trigger_buffer.keys()):
            if node_name in loop_state.pending_tasks:
                continue

            trigger = _pop_trigger(loop_state.trigger_buffer, node_name)
            if not trigger:
                continue

            _started = self._start_node_task(ctx, loop_state, node_name, trigger)

    def _start_node_task(self, ctx: Context, loop_state: _LoopState, node_name: str, trigger: Trigger) -> bool:
        assert self.graph is not None

        node = self._get_static_node_by_name(node_name)
        _is_ternimal = node_name in self.graph.terminal_node_names

        loop_state.pending_tasks[node_name] = asyncio.create_task(
            NodeRunner(node=node, parent_ctx=ctx).run(trigger.input)
        )
        return True

        
    def _seed_start_triggers(
        self,
        loop_state: _LoopState,
        node_input: object | None,
    ) -> None:
        """Seed triggers for START's direct successors."""
        assert self.graph is not None

        start_edges = [e for e in self.graph.edges if e.from_node.name == START.name]
        for edge in start_edges:
            if edge.to_node._requires_all_predecessors:
                loop_state.node_outputs[START.name] = node_input
                self._buffer_barrier_trigger(loop_state, edge.to_node.name)
                continue

            loop_state.trigger_buffer.setdefault(edge.to_node.name, []).append(Trigger(input=node_input))

    def _buffer_barrier_trigger(self, loop_state: _LoopState, target_name: str) -> None:
        assert self.graph is not None

        predecessors = [e for e in self.graph.edges if e.to_node.name == target_name]
        if not all(
            p == START.name
            or (
                loop_state.nodes.get(p)
                and loop_state.nodes[p].status == NodeStatus.COMPLETED
            )
            for p in predecessors
        )
            return

def _pop_trigger(trigger_buffer: TriggerBuffer, node_name: str) -> Trigger | None:
    buffer = trigger_buffer.get(node_name, [])
    if not buffer:
        return None

    trigger = buffer.pop(0)
    if not buffer:
        del trigger_buffer[node_name]

    return trigger