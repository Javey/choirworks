from __future__ import annotations

import uuid

import structlog
from a2a.types.a2a_pb2 import TaskState

from choirworks.a2a.room import RoomOptions
from choirworks.core.planner import PlanDraft, PlanningFailed, plan
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import (
    emit_event,
    emit_function_call,
    emit_function_error,
    emit_text_chunk,
    emit_thought_chunk,
)
from choirworks.orchestration.flows import join_members
from choirworks.orchestration.runner import start_runner
from choirworks.orchestration.state import start_new_plan
from choirworks.tools import FunctionContext, ToolCallResult, create_plan_func

logger = structlog.get_logger(__name__)


async def stream_plan(
    ctx: OrchestrationContext,
    request: str,
    func_ctx: FunctionContext,
    *,
    context_brief: str | None = None,
) -> ToolCallResult:
    """Stream the planning LLM, emitting thought/text chunks, return the tool call."""
    logger.info(
        "stream_plan", task=ctx.task_id, context_id=ctx.context_id, request_len=len(request)
    )
    tool_call: ToolCallResult | None = None
    reasoning_parts: list[str] = []
    content_parts: list[str] = []
    first_reasoning = True
    first_content = True
    thought_id = uuid.uuid4().hex
    text_id = uuid.uuid4().hex
    async for item in plan(
        ctx.llm,
        ctx.registry,
        request,
        ctx=func_ctx,
        context=context_brief,
        max_nodes=ctx.config.max_nodes,
        max_retries=ctx.config.max_plan_retries,
    ):
        if isinstance(item, ToolCallResult):
            tool_call = item
            continue
        reasoning = getattr(item, "reasoning_content", None)
        content = getattr(item, "content", None)
        if reasoning:
            reasoning_parts.append(reasoning)
            await emit_thought_chunk(
                ctx,
                text=reasoning,
                author="assistant",
                append=not first_reasoning,
                last_chunk=False,
                artifact_id=thought_id,
            )
            first_reasoning = False
        if content:
            content_parts.append(content)
            await emit_text_chunk(
                ctx,
                text=content,
                append=not first_content,
                last_chunk=False,
                artifact_id=text_id,
            )
            first_content = False
    reasoning = "".join(reasoning_parts)
    if reasoning:
        await emit_thought_chunk(
            ctx,
            text=reasoning,
            author="assistant",
            append=False,
            last_chunk=True,
            artifact_id=thought_id,
        )
    content = "".join(content_parts)
    if content:
        await emit_text_chunk(
            ctx,
            text=content,
            append=False,
            last_chunk=True,
            artifact_id=text_id,
        )
    if tool_call is None:
        raise PlanningFailed("planner stream ended without a plan")
    logger.info(
        "stream_plan done",
        task=ctx.task_id,
        context_id=ctx.context_id,
        reasoning_len=len(reasoning),
        content_len=len(content),
        tool=tool_call.function.name,
    )
    return tool_call


async def plan_and_launch(
    ctx: OrchestrationContext,
    text: str,
    *,
    room: RoomOptions | None = None,
) -> None:
    """Plan a new turn, create nodes, join members, start the runner."""
    state = ctx.state
    start_new_plan(state, f"plan-{uuid.uuid4().hex[:8]}")
    logger.info(
        "plan_and_launch", task=ctx.task_id, context_id=ctx.context_id, plan_id=state.plan_id
    )
    context_brief = await ctx.brief_builder.build(ctx.context_id, exclude_task_id=ctx.task_id)
    func_ctx = FunctionContext(
        runtime=ctx.runtime,
        registry=ctx.registry,
        effects=ctx.effects,
    )
    try:
        tool_call = await stream_plan(ctx, text, func_ctx, context_brief=context_brief or None)
    except PlanningFailed as exc:
        logger.warning(
            "Planning failed for task", task=ctx.task_id, context_id=ctx.context_id, error=exc
        )
        await ctx.sessions.persist(ctx)
        await emit_function_error(
            ctx,
            create_plan_func,
            str(exc),
            state_name=TaskState.TASK_STATE_FAILED,
        )
        ctx.sessions.evict_session(ctx.context_id)
        return

    draft = (
        tool_call.args
        if isinstance(tool_call.args, PlanDraft)
        else (PlanDraft.model_validate(tool_call.args.model_dump()))
    )

    if not draft.nodes:
        logger.info(
            "plan_and_launch empty plan, completing", task=ctx.task_id, context_id=ctx.context_id
        )
        await ctx.sessions.persist(ctx)
        await emit_event(ctx, "", TaskState.TASK_STATE_COMPLETED)
        ctx.sessions.evict_session(ctx.context_id)
        return

    logger.info(
        "plan_and_launch",
        task=ctx.task_id,
        context_id=ctx.context_id,
        nodes=len(draft.nodes),
        agents=[n.agent_name for n in draft.nodes],
    )
    result = await tool_call.function.execute(func_ctx, tool_call.args)
    await emit_function_call(
        ctx,
        tool_call.function,
        tool_call.args,
        result,
        state_name=TaskState.TASK_STATE_WORKING,
    )

    agents = await ctx.registry.list()
    agent_urls = {agent.name: agent.card_url for agent in agents}
    mention_targets = [name for name in (room or {}).get("mentions", []) if name in agent_urls]
    if mention_targets:
        await join_members(ctx, mention_targets, "human_mention")
    await ctx.sessions.persist(ctx)
    start_runner(ctx)
