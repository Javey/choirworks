from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from types import SimpleNamespace
from typing import Any

from litellm.types.utils import Delta

from choirworks.core.planner import PlanDraft
from choirworks.models.domain import AgentRecord
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.state import OrchestrationState
from choirworks.tools.base import AgentFunction, ToolCallResult
from choirworks.tools.capabilities import ToolEffects


class FakeRegistry:
    """Stub registry exposing only ``list()`` for tool/subagent tests."""

    def __init__(self, agents: Sequence[AgentRecord] | None = None):
        self._agents = list(agents or [])

    async def list(self) -> list[AgentRecord]:
        return list(self._agents)


async def _noop(*args: object, **kwargs: object) -> None:
    return None


class FakeSessions:
    """Minimal SessionManager stand-in: records persist calls."""

    def __init__(self) -> None:
        self.persist_count = 0
        self.evicted: list[str] = []

    async def persist(self, ctx: object) -> None:
        self.persist_count += 1

    def evict_session(self, context_id: str) -> None:
        self.evicted.append(context_id)


def make_orch_ctx(
    registry: object,
    llm: object | None = None,
    *,
    max_derived_nodes: int = 5,
) -> OrchestrationContext:
    """Build an OrchestrationContext for a fake executor: no session, no-op effects."""
    runtime = SimpleNamespace(
        state=OrchestrationState(),
        task_id="t1",
        context_id="c1",
        queue=None,
        lock=None,
    )
    effects = ToolEffects(
        max_derived_nodes=max_derived_nodes,
        join_members=_noop,
        persist=_noop,
        apply_patch_locked=_noop,  # type: ignore[arg-type]
    )
    return OrchestrationContext(
        runtime=runtime,  # type: ignore[arg-type]
        registry=registry,  # type: ignore[arg-type]
        remote=SimpleNamespace(),  # type: ignore[arg-type]
        llm=llm if llm is not None else SimpleNamespace(),  # type: ignore[arg-type]
        sessions=FakeSessions(),  # type: ignore[arg-type]
        config=SimpleNamespace(max_derived_nodes=max_derived_nodes),  # type: ignore[arg-type]
        brief_builder=SimpleNamespace(),  # type: ignore[arg-type]
        effects=effects,
    )


class FakeLLM:
    """Mock LLM client for testing Planner / ContextBriefBuilder.

    Pass ``structured_results`` (list of Pydantic models) to script
    ``stream()`` calls in order, and ``text_results`` to script
    ``text()`` calls.
    """

    def __init__(
        self,
        structured_results: list[Any] | None = None,
        text_results: list[str] | None = None,
    ):
        self.structured_results = list(structured_results or [])
        self.text_results = list(text_results or [])
        self.stream_calls: list[dict[str, Any]] = []
        self.text_calls: list[dict[str, Any]] = []

    async def stream(
        self,
        *,
        system: str,
        user: str,
        tools: list[AgentFunction] | None = None,
        ctx: OrchestrationContext | None = None,
        tool_choice: str | dict[str, object] = "auto",
    ) -> AsyncIterator[Delta | ToolCallResult]:
        """Direct LLM client interface — streams reasoning deltas then the tool call."""
        self.stream_calls.append(
            {
                "system": system,
                "user": user,
                "tools": tools,
                "ctx": ctx,
                "tool_choice": tool_choice,
            }
        )
        if not self.structured_results:
            raise AssertionError("FakeLLM has no scripted structured result")
        result = self.structured_results.pop(0)
        if isinstance(result, Exception):
            raise result
        reasoning = (
            f"思考：将请求拆解为 {len(result.nodes)} 个节点。"
            if isinstance(result, PlanDraft)
            else "思考：解读产出。"
        )
        midpoint = len(reasoning) // 2
        yield Delta(reasoning_content=reasoning[:midpoint])
        yield Delta(reasoning_content=reasoning[midpoint:])
        if tools:
            yield ToolCallResult(function=tools[0], args=result)

    async def text(self, *, system: str, user: str) -> str:
        """Direct LLM client interface — used by ContextBriefBuilder."""
        self.text_calls.append({"system": system, "user": user})
        if not self.text_results:
            raise AssertionError("FakeLLM has no scripted text result")
        return self.text_results.pop(0)

    def count_tokens(self, text: str) -> int:
        return len(text) // 3

    def get_context_window(self) -> int:
        return 128000
