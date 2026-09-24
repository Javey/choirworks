from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

import pytest

from choirworks.orchestration.flows.engine import (
    DEFAULT,
    Edge,
    Flow,
    FlowError,
    FlowOutcome,
    Handler,
)

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

CTX = cast("OrchestrationContext", object())


def _flow(
    handlers: dict[str, Handler[list[str]]],
    edges: tuple[Edge, ...],
    *,
    start: str = "start",
) -> Flow[list[str]]:
    flow: Flow[list[str]] = Flow(name="test", start=start, handlers=handlers, edges=edges)
    flow.validate()
    return flow


async def test_unconditional_chain_runs_to_end():
    async def a(ctx: object, payload: list[str]) -> str:
        payload.append("a")
        return ""

    async def b(ctx: object, payload: list[str]) -> FlowOutcome:
        payload.append("b")
        return FlowOutcome.EXIT_DONE

    flow = _flow({"start": a, "b": b}, (Edge("start", "b"),))
    payload: list[str] = []

    outcome = await flow.run(CTX, payload)

    assert payload == ["a", "b"]
    assert outcome is FlowOutcome.EXIT_DONE


async def test_specific_route_beats_unconditional_fallthrough_and_default():
    async def check(ctx: object, payload: list[str]) -> str:
        return payload.pop()

    async def hit(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.EXIT_DONE

    async def fallthrough(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.EXIT_WAIT

    async def default(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.EXIT_FAILED

    edges = (
        Edge("check", "hit", frozenset({"go"})),
        Edge("check", "fallthrough"),
        Edge("check", "default", frozenset({DEFAULT})),
    )
    handlers: dict[str, Handler[list[str]]] = {
        "check": check,
        "hit": hit,
        "fallthrough": fallthrough,
        "default": default,
    }
    flow: Flow[list[str]] = Flow(name="test", start="check", handlers=handlers, edges=edges)
    flow.validate()

    assert await flow.run(CTX, ["go"]) is FlowOutcome.EXIT_DONE
    assert await flow.run(CTX, ["other"]) is FlowOutcome.EXIT_WAIT


async def test_no_matching_edge_ends_the_branch():
    async def only(ctx: object, payload: list[str]) -> str:
        return "nothing"

    flow = _flow({"start": only}, ())
    assert await flow.run(CTX, []) is FlowOutcome.END


async def test_cycle_is_supported():
    async def loop(ctx: object, payload: list[str]) -> str | FlowOutcome:
        if len(payload) >= 3:
            return FlowOutcome.EXIT_DONE
        payload.append(str(len(payload)))
        return "again"

    flow = _flow({"start": loop}, (Edge("start", "start", frozenset({"again"})),))
    payload: list[str] = []

    outcome = await flow.run(CTX, payload)

    assert outcome is FlowOutcome.EXIT_DONE
    assert len(payload) == 3


async def test_dict_payload_flows_through():
    async def h(ctx: object, payload: dict[str, str]) -> FlowOutcome:
        payload["seen"] = "yes"
        return FlowOutcome.EXIT_DONE

    flow: Flow[dict[str, str]] = Flow(name="t", start="a", handlers={"a": h}, edges=())
    flow.validate()
    payload: dict[str, str] = {}

    assert await flow.run(CTX, payload) is FlowOutcome.EXIT_DONE
    assert payload == {"seen": "yes"}


def test_validate_rejects_unknown_start():
    async def a(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    flow: Flow[list[str]] = Flow(name="t", start="missing", handlers={"a": a}, edges=())
    with pytest.raises(FlowError):
        flow.validate()


def test_validate_rejects_unknown_edge_endpoint():
    async def a(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    flow: Flow[list[str]] = Flow(
        name="t", start="a", handlers={"a": a}, edges=(Edge("a", "ghost"),)
    )
    with pytest.raises(FlowError):
        flow.validate()


def test_validate_rejects_duplicate_edge_and_multiple_defaults():
    async def a(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    async def b(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    handlers: dict[str, Handler[list[str]]] = {"a": a, "b": b}
    duplicate: Flow[list[str]] = Flow(
        name="t", start="a", handlers=handlers, edges=(Edge("a", "b"), Edge("a", "b"))
    )
    with pytest.raises(FlowError):
        duplicate.validate()

    multi_default: Flow[list[str]] = Flow(
        name="t",
        start="a",
        handlers=handlers,
        edges=(
            Edge("a", "b", frozenset({DEFAULT})),
            Edge("a", "a", frozenset({DEFAULT})),
        ),
    )
    with pytest.raises(FlowError):
        multi_default.validate()


def test_validate_rejects_unreachable_handler():
    async def a(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    async def orphan(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    flow: Flow[list[str]] = Flow(name="t", start="a", handlers={"a": a, "orphan": orphan}, edges=())
    with pytest.raises(FlowError):
        flow.validate()


def test_validate_rejects_literal_route_without_edge():
    async def a(ctx: object, payload: list[str]) -> Literal["go"] | FlowOutcome:
        return "go"

    flow: Flow[list[str]] = Flow(name="t", start="a", handlers={"a": a}, edges=())
    with pytest.raises(FlowError):
        flow.validate()
