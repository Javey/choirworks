# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_graph.py、utils/_graph_validation.py
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Literal, cast, get_args, get_origin, get_type_hints

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

DEFAULT = "__default__"

type Route = str


class FlowOutcome(StrEnum):
    """Terminal action a flow reports back to its caller."""

    CONTINUE = "continue"
    EXIT_WAIT = "exit_wait"
    EXIT_DONE = "exit_done"
    EXIT_FAILED = "exit_failed"
    END = "end"


type Handler[P] = Callable[[OrchestrationContext, P], Awaitable[Route | FlowOutcome]]


class FlowError(ValueError):
    """A malformed flow definition (name/edge/route problems)."""


@dataclass(frozen=True, slots=True)
class Edge:
    """A declarative transition: ``source`` emits a route that ``routes`` catches.

    An empty ``routes`` set is an unconditional edge; ``DEFAULT`` is the
    fallback taken when no specific route edge matched.
    """

    source: str
    target: str
    routes: frozenset[Route] = field(default_factory=frozenset)


@dataclass(slots=True)
class Flow[P]:
    """A linear route-tagged flow (design mirror of ADK's Workflow routing)."""

    name: str
    start: str
    handlers: dict[str, Handler[P]]
    edges: tuple[Edge, ...]

    def validate(self) -> None:
        if self.start not in self.handlers:
            raise FlowError(f"{self.name}: unknown start handler {self.start!r}")
        seen_edges: set[tuple[str, str]] = set()
        defaults_per_source: dict[str, int] = {}
        for edge in self.edges:
            if edge.source not in self.handlers:
                raise FlowError(f"{self.name}: edge source {edge.source!r} unknown")
            if edge.target not in self.handlers:
                raise FlowError(f"{self.name}: edge target {edge.target!r} unknown")
            key = (edge.source, edge.target)
            if key in seen_edges:
                raise FlowError(f"{self.name}: duplicate edge {edge.source}->{edge.target}")
            seen_edges.add(key)
            if DEFAULT in edge.routes:
                defaults_per_source[edge.source] = defaults_per_source.get(edge.source, 0) + 1
        for source, count in defaults_per_source.items():
            if count > 1:
                raise FlowError(f"{self.name}: multiple DEFAULT edges from {source!r}")

        unreachable = set(self.handlers) - self._reachable()
        if unreachable:
            raise FlowError(f"{self.name}: unreachable handlers {sorted(unreachable)}")

        for name, handler in self.handlers.items():
            for route in _literal_routes(handler):
                if self._target_for(name, route) is None:
                    raise FlowError(f"{self.name}: handler {name!r} emits {route!r} with no edge")

    async def run(self, ctx: OrchestrationContext, payload: P) -> FlowOutcome:
        current = self.start
        while True:
            result = await self.handlers[current](ctx, payload)
            if isinstance(result, FlowOutcome):
                return result
            target = self._target_for(current, result)
            if target is None:
                return FlowOutcome.END
            current = target

    def _target_for(self, source: str, route: Route) -> str | None:
        specific: str | None = None
        unconditional: str | None = None
        fallback: str | None = None
        for edge in self.edges:
            if edge.source != source:
                continue
            if not edge.routes:
                unconditional = unconditional or edge.target
            elif route in edge.routes:
                specific = specific or edge.target
            elif DEFAULT in edge.routes:
                fallback = fallback or edge.target
        return specific or unconditional or fallback

    def _reachable(self) -> set[str]:
        reached = {self.start}
        frontier = [self.start]
        while frontier:
            source = frontier.pop()
            for edge in self.edges:
                if edge.source == source and edge.target not in reached:
                    reached.add(edge.target)
                    frontier.append(edge.target)
        return reached


def _literal_routes[P](handler: Handler[P]) -> set[Route]:
    try:
        hints = get_type_hints(handler)
    except (NameError, TypeError):
        return set()
    annotation: object | None = hints.get("return")
    if annotation is None:
        return set()
    return _collect_literals(annotation)


def _collect_literals(annotation: object) -> set[str]:
    args = cast("tuple[object, ...]", get_args(annotation))
    if get_origin(annotation) is Literal:
        return {value for value in args if isinstance(value, str)}
    routes: set[str] = set()
    for arg in args:
        routes |= _collect_literals(arg)
    return routes
