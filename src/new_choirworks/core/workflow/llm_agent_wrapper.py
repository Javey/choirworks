# 设计参考 google-adk workflow/_llm_agent_wrapper.py（Apache-2.0, Copyright 2026 Google LLC）：
# 精简版：无 mode/session/resume。single_turn 语义——节点按 include_contents 组装上下文；
# 终态 artifact 的文本按 output_schema 校验后提升为节点输出。
from __future__ import annotations

import copy
import json
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

from new_choirworks.core.events.event import A2AEvent
from new_choirworks.core.workflow.context import Context

if TYPE_CHECKING:
    from new_choirworks.core.agents.llm_agent import LLMAgent


def prepare_llm_agent_context(agent: LLMAgent, ctx: Context) -> Context:
    """Prepares the context for the agent node.

    include_contents='default'（显式开启）：带父级会话历史快照；
    'none'（节点默认）：不带历史，只看本次 node_input。
    """
    ic = copy.copy(ctx.invocation_context)
    ic.messages = (
        list(ctx.invocation_context.messages)
        if agent.include_contents == "default"
        else []
    )
    return Context(invocation_context=ic, node=agent)


def prepare_llm_agent_input(
    _agent: LLMAgent, ctx: Context, node_input: object | None
) -> None:
    """Injects the node input as the agent's only user message."""
    if node_input is None:
        return
    agent_input = (
        node_input
        if isinstance(node_input, str)
        else json.dumps(node_input, ensure_ascii=False, default=str)
    )
    ctx.invocation_context.messages.append({"role": "user", "content": agent_input})


def process_llm_agent_output(agent: LLMAgent, ctx: Context, event: A2AEvent) -> None:
    """Promotes the final artifact's text to the node output."""
    raise NotImplementedError("process_llm_agent_output is not implemented.")


async def run_llm_agent_as_node(
    agent: LLMAgent, *, ctx: Context, node_input: object | None
) -> AsyncGenerator[A2AEvent, None]:
    """Runs an LLMAgent as a workflow node."""
    raise NotImplementedError("run_llm_agent_as_node is not implemented.")
    yield  # pyright: ignore[reportUnreachable]
