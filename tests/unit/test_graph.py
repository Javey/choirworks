from __future__ import annotations

from typing import TYPE_CHECKING, Literal, cast

import pytest

from choirworks.orchestration.flows.engine import (
    DEFAULT,
    Edge,
    Flow,
    FlowError,
    FlowOutcome,
)

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

CTX = cast("OrchestrationContext", object())


async def test_unconditional_chain_runs_to_end():
    async def a(ctx: object, payload: list[str]) -> str:
        payload.append("a")
        return ""

    async def b(ctx: object, payload: list[str]) -> FlowOutcome:
        payload.append("b")
        return FlowOutcome.EXIT_DONE

    flow: Flow[list[str]] = Flow(name="test", start=a, edges=(Edge(a, b),))
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

    flow: Flow[list[str]] = Flow(
        name="test",
        start=check,
        edges=(
            Edge(check, hit, frozenset({"go"})),
            Edge(check, fallthrough),
            Edge(check, default, frozenset({DEFAULT})),
        ),
    )

    assert await flow.run(CTX, ["go"]) is FlowOutcome.EXIT_DONE
    assert await flow.run(CTX, ["other"]) is FlowOutcome.EXIT_WAIT


async def test_no_matching_edge_ends_the_branch():
    async def only(ctx: object, payload: list[str]) -> str:
        return "nothing"

    flow: Flow[list[str]] = Flow(name="test", start=only, edges=())
    assert await flow.run(CTX, []) is FlowOutcome.END


async def test_cycle_is_supported():
    async def loop(ctx: object, payload: list[str]) -> str | FlowOutcome:
        if len(payload) >= 3:
            return FlowOutcome.EXIT_DONE
        payload.append(str(len(payload)))
        return "again"

    flow: Flow[list[str]] = Flow(
        name="test", start=loop, edges=(Edge(loop, loop, frozenset({"again"})),)
    )
    payload: list[str] = []

    outcome = await flow.run(CTX, payload)

    assert outcome is FlowOutcome.EXIT_DONE
    assert len(payload) == 3


async def test_shared_handler_target_merges_routes():
    async def check(ctx: object, payload: list[str]) -> str:
        return payload.pop()

    async def stop(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    flow: Flow[list[str]] = Flow(
        name="test",
        start=check,
        edges=(Edge(check, stop, frozenset({"a", "b"})),),
    )

    assert await flow.run(CTX, ["a"]) is FlowOutcome.END
    assert await flow.run(CTX, ["b"]) is FlowOutcome.END


async def test_dict_payload_flows_through():
    async def h(ctx: object, payload: dict[str, str]) -> FlowOutcome:
        payload["seen"] = "yes"
        return FlowOutcome.EXIT_DONE

    flow: Flow[dict[str, str]] = Flow(name="t", start=h, edges=())
    payload: dict[str, str] = {}

    assert await flow.run(CTX, payload) is FlowOutcome.EXIT_DONE
    assert payload == {"seen": "yes"}


def test_construction_rejects_duplicate_edge():
    async def a(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    async def b(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    with pytest.raises(FlowError):
        Flow(name="t", start=a, edges=(Edge(a, b), Edge(a, b)))


def test_construction_rejects_multiple_defaults_per_source():
    async def a(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    async def b(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    with pytest.raises(FlowError):
        Flow(
            name="t",
            start=a,
            edges=(
                Edge(a, b, frozenset({DEFAULT})),
                Edge(a, a, frozenset({DEFAULT})),
            ),
        )


def test_construction_rejects_unreachable_handler():
    async def a(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    async def b(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    async def c(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    async def d(ctx: object, payload: list[str]) -> FlowOutcome:
        return FlowOutcome.END

    with pytest.raises(FlowError):
        Flow(name="t", start=a, edges=(Edge(a, b), Edge(c, d)))


def test_construction_rejects_literal_route_without_edge():
    async def a(ctx: object, payload: list[str]) -> Literal["go"] | FlowOutcome:
        return "go"

    with pytest.raises(FlowError):
        Flow(name="t", start=a, edges=())
