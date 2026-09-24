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


def _label[P](handler: Handler[P]) -> str:
    return getattr(handler, "__name__", repr(handler))


@dataclass(frozen=True, slots=True)
class Edge[P]:
    """A declarative transition; endpoints are handler functions (ADK-style).

    An empty ``routes`` set is an unconditional edge; ``DEFAULT`` is the
    fallback taken when no specific route edge matched.
    """

    source: Handler[P]
    target: Handler[P]
    routes: frozenset[Route] = field(default_factory=frozenset)


@dataclass(slots=True)
class Flow[P]:
    """A linear route-tagged flow (design mirror of ADK's Workflow routing)."""

    name: str
    start: Handler[P]
    edges: tuple[Edge[P], ...]

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        seen_edges: set[tuple[Handler[P], Handler[P]]] = set()
        defaults_per_source: dict[Handler[P], int] = {}
        for edge in self.edges:
            key = (edge.source, edge.target)
            if key in seen_edges:
                raise FlowError(
                    f"{self.name}: duplicate edge {_label(edge.source)}->{_label(edge.target)}"
                )
            seen_edges.add(key)
            if DEFAULT in edge.routes:
                defaults_per_source[edge.source] = defaults_per_source.get(edge.source, 0) + 1
        for source, count in defaults_per_source.items():
            if count > 1:
                raise FlowError(f"{self.name}: multiple DEFAULT edges from {_label(source)}")

        unreachable = self._nodes() - self._reachable()
        if unreachable:
            raise FlowError(f"{self.name}: unreachable handlers {sorted(map(_label, unreachable))}")

        for handler in self._nodes():
            for route in _literal_routes(handler):
                if self._target_for(handler, route) is None:
                    raise FlowError(
                        f"{self.name}: handler {_label(handler)} emits {route!r} with no edge"
                    )

    async def run(self, ctx: OrchestrationContext, payload: P) -> FlowOutcome:
        current = self.start
        while True:
            result = await current(ctx, payload)
            if isinstance(result, FlowOutcome):
                return result
            target = self._target_for(current, result)
            if target is None:
                return FlowOutcome.END
            current = target

    def _nodes(self) -> set[Handler[P]]:
        nodes = {self.start}
        for edge in self.edges:
            nodes.add(edge.source)
            nodes.add(edge.target)
        return nodes

    def _target_for(self, source: Handler[P], route: Route) -> Handler[P] | None:
        specific: Handler[P] | None = None
        unconditional: Handler[P] | None = None
        fallback: Handler[P] | None = None
        for edge in self.edges:
            if edge.source is not source:
                continue
            if not edge.routes:
                unconditional = unconditional or edge.target
            elif route in edge.routes:
                specific = specific or edge.target
            elif DEFAULT in edge.routes:
                fallback = fallback or edge.target
        return specific or unconditional or fallback

    def _reachable(self) -> set[Handler[P]]:
        reached = {self.start}
        frontier = [self.start]
        while frontier:
            source = frontier.pop()
            for edge in self.edges:
                if edge.source is source and edge.target not in reached:
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
