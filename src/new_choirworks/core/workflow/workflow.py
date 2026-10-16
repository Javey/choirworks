# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_workflow.py
from __future__ import annotations

import asyncio
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

        self._finalize(loop_state, ctx)

        return
        yield  # pyright: ignore[reportUnreachable]

    async def _run_loop(self, ctx: Context, loop_state: _LoopState) -> None:
        while True:
            await self._schedule_ready_nodes(ctx, loop_state)

            if not loop_state.pending_tasks:
                break

            wait_tasks = list(loop_state.pending_tasks.values())

            done, _ = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
            order_done = [task for task in loop_state.pending_tasks.values() if task in done]

            for task in order_done:
                name = self._pop_completed_task(loop_state, task)

                child_ctx = task.result()

                if child_ctx.error:
                    loop_state.nodes[name].status = NodeStatus.FAILED
                    raise WorkflowError(f"node {name} failed: {child_ctx.error}")
                else:
                    await self._handle_completion(loop_state, name, child_ctx)

    async def _schedule_ready_nodes(self, ctx: Context, loop_state: _LoopState) -> None:
        for node_name in list(loop_state.trigger_buffer.keys()):
            if node_name in loop_state.pending_tasks:
                continue

            node_state = loop_state.nodes.get(node_name)
            if node_state:
                if node_state.status == NodeStatus.RUNNING:
                    continue

            if self._at_concurrency_limit(loop_state):
                break

            trigger = _pop_trigger(loop_state.trigger_buffer, node_name)
            if not trigger:
                continue

            self._prepare_node_state_for_starting(loop_state, node_name, trigger)
            _started = self._start_node_task(ctx, loop_state, node_name, trigger)

    def _at_concurrency_limit(self, loop_state: _LoopState) -> bool:
        """Check if max_concurrency has been reached."""
        return (
            self.max_concurrency is not None
            and self.max_concurrency > 0
            and len(loop_state.pending_tasks) >= self.max_concurrency
        )

    def _prepare_node_state_for_starting(
        self,
        loop_state: _LoopState,
        node_name: str,
        trigger: Trigger,
    ) -> None:
        """Prepare NodeState for starting a node."""
        loop_state.nodes[node_name] = NodeState(
            status=NodeStatus.RUNNING,
            input=trigger.input,
        )

    def _start_node_task(
        self, ctx: Context, loop_state: _LoopState, node_name: str, trigger: Trigger
    ) -> bool:
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

            loop_state.trigger_buffer.setdefault(edge.to_node.name, []).append(
                Trigger(input=node_input)
            )

    def _buffer_barrier_trigger(self, loop_state: _LoopState, target_name: str) -> None:
        assert self.graph is not None

        predecessors = {e.from_node.name for e in self.graph.edges if e.to_node.name == target_name}
        if not all(
            p == START.name or ((s := loop_state.nodes.get(p)) and s.status == NodeStatus.COMPLETED)
            for p in predecessors
        ):
            return

        outputs = {p: loop_state.node_outputs.get(p) for p in predecessors}
        loop_state.trigger_buffer.setdefault(target_name, []).append(Trigger(input=outputs))

    def _pop_completed_task(self, loop_state: _LoopState, task: asyncio.Task[Context]) -> str:
        for name, t in loop_state.pending_tasks.items():
            if t is task:
                del loop_state.pending_tasks[name]
                return name

        raise WorkflowError(f"task {task} not found in pending_tasks")

    async def _handle_completion(
        self, loop_state: _LoopState, node_name: str, child_ctx: Context
    ) -> None:
        node_state = loop_state.nodes[node_name]
        node_state.status = NodeStatus.COMPLETED

        if child_ctx.output is not None:
            loop_state.node_outputs[node_name] = child_ctx.output

        self._buffer_downstream_triggers(loop_state, node_name, child_ctx)

    def _buffer_downstream_triggers(
        self, loop_state: _LoopState, node_name: str, child_ctx: Context
    ) -> None:
        assert self.graph is not None

        next_nodes = self.graph.get_next_pending_nodes(node_name, routes_to_match=child_ctx.route)
        for target_name in next_nodes:
            target_node = self._get_static_node_by_name(target_name)

            if target_node._requires_all_predecessors:
                self._buffer_barrier_trigger(loop_state, target_name)
            else:
                loop_state.trigger_buffer.setdefault(target_name, []).append(
                    Trigger(input=child_ctx.output)
                )

    def _finalize(self, loop_state: _LoopState, ctx: Context) -> None:
        """Set the terminal node's output on ctx so the parent can read it."""
        assert self.graph is not None

        terminal_outputs = [
            loop_state.node_outputs[name]
            for name in self.graph.terminal_node_names
            if name in loop_state.node_outputs
        ]
        if len(terminal_outputs) == 1:
            ctx.output = terminal_outputs[0]
        elif terminal_outputs:
            raise WorkflowError(
                f"Workflow {self.name}: multiple terminal nodes produced"
                f" output ({len(terminal_outputs)}). A workflow must have"
                " at most one terminal output."
            )


def _pop_trigger(trigger_buffer: TriggerBuffer, node_name: str) -> Trigger | None:
    buffer = trigger_buffer.get(node_name, [])
    if not buffer:
        return None

    trigger = buffer.pop(0)
    if not buffer:
        del trigger_buffer[node_name]

    return trigger
