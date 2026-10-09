from __future__ import annotations

from types import SimpleNamespace

from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent
from litellm.types.utils import Delta
from pydantic import BaseModel

from choirworks.core.agents.llm_agent import LlmAgent
from choirworks.core.events import THOUGHT
from choirworks.tools.base import AgentFunction, FunctionResult, ToolCallResult


class _DecisionArgs(BaseModel):
    answer: str


async def _args_model(ctx: object) -> type[BaseModel]:
    return _DecisionArgs


async def _execute(ctx: object, args: BaseModel) -> FunctionResult:
    return FunctionResult(success=True)


_DECISION_TOOL = AgentFunction(
    name="decide",
    description="decide something",
    args_model=_args_model,
    execute=_execute,
)


async def _build_tools(ctx: object, **kwargs: object) -> list[AgentFunction]:
    return [_DECISION_TOOL]


def _process(tool_call: ToolCallResult | None) -> str:
    if tool_call is None:
        return "fallback"
    args = tool_call.args if isinstance(tool_call.args, _DecisionArgs) else None
    return args.answer if args else "unparsed"


class _FakeLLM:
    def __init__(self, attempts: list[list[object]]) -> None:
        self._attempts = attempts
        self.calls: list[dict[str, object]] = []

    async def stream(
        self,
        *,
        system: str,
        user: str,
        tools: list[AgentFunction] | None = None,
        ctx: object = None,
        tool_choice: str | dict[str, object] = "auto",
    ):
        self.calls.append(
            {"system": system, "user": user, "tools": tools, "tool_choice": tool_choice}
        )
        for item in self._attempts.pop(0):
            yield item


class _Queue:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def enqueue_event(self, event: object) -> None:
        self.events.append(event)


def _agent(
    name: str = "oracle",
    max_retries: int = 2,
    sub_agents: list[LlmAgent[str]] | None = None,
) -> LlmAgent[str]:
    return LlmAgent(
        name=name,
        system_prompt="SYS",
        tool_name="decide",
        build_tools=_build_tools,
        process=_process,
        max_retries=max_retries,
        sub_agents=sub_agents or [],
    )


def _ctx(llm: _FakeLLM) -> tuple[SimpleNamespace, _Queue]:
    queue = _Queue()
    ctx = SimpleNamespace(task_id="t1", context_id="c1", queue=queue, llm=llm)
    return ctx, queue


def _thought_chunks(queue: _Queue) -> list[tuple[str, bool, bool]]:
    chunks: list[tuple[str, bool, bool]] = []
    for event in queue.events:
        if not isinstance(event, TaskArtifactUpdateEvent):
            continue
        part = event.artifact.parts[0]
        kind = part.metadata.fields.get("cw_type")
        if kind is None or kind.string_value != THOUGHT:
            continue
        chunks.append((part.text, event.append, event.last_chunk))
    return chunks


async def test_run_async_streams_and_returns_processed_result():
    llm = _FakeLLM(
        [
            [
                Delta(reasoning_content="思考一"),
                ToolCallResult(function=_DECISION_TOOL, args=_DecisionArgs(answer="ok")),
            ]
        ]
    )
    agent = _agent()
    ctx, queue = _ctx(llm)

    result = await agent.run_async(ctx, "问")  # type: ignore[arg-type]

    assert result == "ok"
    call = llm.calls[0]
    assert call["system"] == "SYS"
    assert call["user"] == "问"
    assert call["tool_choice"] == {"type": "function", "function": {"name": "decide"}}
    assert _thought_chunks(queue) == [("思考一", False, False), ("思考一", False, True)]


async def test_run_async_retries_with_feedback_and_streams_both_attempts():
    llm = _FakeLLM(
        [
            [Delta(reasoning_content="第一轮思考")],
            [
                Delta(reasoning_content="第二轮思考"),
                ToolCallResult(function=_DECISION_TOOL, args=_DecisionArgs(answer="好")),
            ],
        ]
    )
    agent = _agent()
    ctx, queue = _ctx(llm)

    result = await agent.run_async(ctx, "问")  # type: ignore[arg-type]

    assert result == "好"
    assert len(llm.calls) == 2
    assert "Your previous tool call was invalid" in llm.calls[1]["user"]
    chunks = _thought_chunks(queue)
    assert ("第一轮思考", False, True) in chunks
    assert ("第二轮思考", False, True) in chunks


async def test_run_async_exhausts_retries_into_process_fallback():
    llm = _FakeLLM([[Delta(reasoning_content="想")], [Delta(reasoning_content="再想")]])
    agent = _agent(max_retries=1)
    ctx, _queue = _ctx(llm)

    result = await agent.run_async(ctx, "问")  # type: ignore[arg-type]

    assert result == "fallback"
    assert len(llm.calls) == 2


async def test_llm_agent_joins_agent_tree():
    child = _agent(name="child")
    root = _agent(name="root", sub_agents=[child])

    assert child.parent_agent is root
