from __future__ import annotations

from types import SimpleNamespace
from typing import override

from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent
from litellm.types.utils import Delta
from pydantic import BaseModel

from choirworks.core.agents.context import TurnContext
from choirworks.core.agents.llm_agent import LlmAgent
from choirworks.core.events import TEXT, THOUGHT
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


class _DecisionAgent(LlmAgent[str]):
    name = "oracle"
    system_prompt = "SYS"
    final_tool = "decide"
    max_retries = 2

    @override
    async def build_tools(self, ctx: TurnContext, **kwargs: object) -> list[AgentFunction]:
        return [_DECISION_TOOL]

    @override
    def process(self, tool_call: ToolCallResult | None) -> str:
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


def _text_agent(name: str = "scribe") -> LlmAgent[str]:
    return LlmAgent(name=name, system_prompt="SYS")


def _ctx(llm: _FakeLLM) -> tuple[SimpleNamespace, _Queue]:
    queue = _Queue()
    ctx = SimpleNamespace(task_id="t1", context_id="c1", queue=queue, llm=llm)
    return ctx, queue


def _chunks(queue: _Queue, kind: str) -> list[tuple[str, bool, bool]]:
    chunks: list[tuple[str, bool, bool]] = []
    for event in queue.events:
        if not isinstance(event, TaskArtifactUpdateEvent):
            continue
        part = event.artifact.parts[0]
        cw_type = part.metadata.fields.get("cw_type")
        if cw_type is None or cw_type.string_value != kind:
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
    agent = _DecisionAgent()
    ctx, queue = _ctx(llm)

    result = await agent.run_async(ctx, "问")  # type: ignore[arg-type]

    assert result == "ok"
    call = llm.calls[0]
    assert call["system"] == "SYS"
    assert call["user"] == "问"
    assert call["tool_choice"] == {"type": "function", "function": {"name": "decide"}}
    assert _chunks(queue, THOUGHT) == [("思考一", False, False), ("思考一", False, True)]


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
    agent = _DecisionAgent()
    ctx, queue = _ctx(llm)

    result = await agent.run_async(ctx, "问")  # type: ignore[arg-type]

    assert result == "好"
    assert len(llm.calls) == 2
    assert "Your previous tool call was invalid" in llm.calls[1]["user"]
    chunks = _chunks(queue, THOUGHT)
    assert ("第一轮思考", False, True) in chunks
    assert ("第二轮思考", False, True) in chunks


async def test_run_async_exhausts_retries_into_process_fallback():
    llm = _FakeLLM([[Delta(reasoning_content="想")], [Delta(reasoning_content="再想")]])
    agent = _DecisionAgent(max_retries=1)
    ctx, _queue = _ctx(llm)

    result = await agent.run_async(ctx, "问")  # type: ignore[arg-type]

    assert result == "fallback"
    assert len(llm.calls) == 2


async def test_llm_agent_joins_agent_tree():
    child = _DecisionAgent(name="child")
    root = _DecisionAgent(name="root", sub_agents=[child])

    assert child.parent_agent is root


async def test_text_mode_streams_and_returns_joined_text_without_tools():
    llm = _FakeLLM(
        [
            [
                Delta(reasoning_content="想"),
                Delta(content="你好"),
                Delta(content="！"),
            ]
        ]
    )
    agent = _text_agent()
    ctx, queue = _ctx(llm)

    result = await agent.run_async(ctx, "问")  # type: ignore[arg-type]

    assert result == "你好！"
    call = llm.calls[0]
    assert call["tools"] is None
    assert call["tool_choice"] == "auto"
    assert _chunks(queue, THOUGHT) == [("想", False, False), ("想", False, True)]
    assert _chunks(queue, TEXT) == [
        ("你好", False, False),
        ("！", True, False),
        ("你好！", False, True),
    ]


async def test_text_mode_is_default_when_final_tool_unset():
    agent = LlmAgent(name="plain", system_prompt="SYS")
    assert agent.final_tool is None
