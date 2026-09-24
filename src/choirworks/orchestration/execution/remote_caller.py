from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import structlog
from a2a.types import StreamResponse
from a2a.types.a2a_pb2 import (
    Artifact,
    Part,
    TaskArtifactUpdateEvent,
    TaskState,
)

from choirworks.a2a.wire import join_text, struct
from choirworks.orchestration.context import OrchestrationContext
from choirworks.orchestration.events import emit_state_delta
from choirworks.orchestration.state import NodeState, NodeStatus
from choirworks.orchestration.transitions import apply_transition

logger = structlog.get_logger(__name__)


_REMOTE_STATE_MAP: dict[int, NodeStatus] = {
    TaskState.TASK_STATE_SUBMITTED: NodeStatus.SUBMITTED,
    TaskState.TASK_STATE_WORKING: NodeStatus.WORKING,
    TaskState.TASK_STATE_INPUT_REQUIRED: NodeStatus.INPUT_REQUIRED,
    TaskState.TASK_STATE_AUTH_REQUIRED: NodeStatus.INPUT_REQUIRED,
    TaskState.TASK_STATE_COMPLETED: NodeStatus.COMPLETED,
    TaskState.TASK_STATE_FAILED: NodeStatus.FAILED,
    TaskState.TASK_STATE_CANCELED: NodeStatus.CANCELED,
    TaskState.TASK_STATE_REJECTED: NodeStatus.FAILED,
    TaskState.TASK_STATE_UNSPECIFIED: NodeStatus.WORKING,
}


async def stream_remote(
    ctx: OrchestrationContext,
    node: NodeState,
    text: str | list[str],
    *,
    continuation: bool,
) -> NodeStatus:
    remote_task_id = node.a2a_task_id if continuation else None
    text_list = text if isinstance(text, list) else [text]
    logger.info(
        "stream_remote",
        context_id=ctx.context_id,
        node_id=node.id,
        agent_url=node.agent_url,
        parts=len(text_list),
        continuation=continuation,
    )
    chunks = ctx.remote.send_text(
        node.agent_url,
        text,
        task_id=remote_task_id,
        context_id=ctx.context_id,
        message_id=f"{ctx.context_id}:{node.id}:{node.attempt}",
    )
    current = await consume_chunks(ctx, node, chunks)
    logger.info("stream_remote done", context_id=ctx.context_id, node_id=node.id, state=current)
    return await ensure_terminal(ctx, node, current)


async def recover_remote(ctx: OrchestrationContext, node: NodeState) -> NodeStatus:
    if not node.a2a_task_id:
        logger.warning("recover_remote no a2a_task_id", context_id=ctx.context_id, node_id=node.id)
        return NodeStatus.FAILED
    logger.info(
        "recover_remote",
        context_id=ctx.context_id,
        node_id=node.id,
        a2a_task_id=node.a2a_task_id,
    )
    current = NodeStatus.WORKING
    try:
        chunks = ctx.remote.subscribe_task(node.agent_url, node.a2a_task_id)
        current = await consume_chunks(ctx, node, chunks)
    except Exception as exc:
        logger.debug(
            "Recover subscribe failed", context_id=ctx.context_id, node_id=node.id, error=exc
        )
    return await ensure_terminal(ctx, node, current)


async def ensure_terminal(
    ctx: OrchestrationContext,
    node: NodeState,
    current: NodeStatus,
) -> NodeStatus:
    settled = {
        NodeStatus.COMPLETED,
        NodeStatus.FAILED,
        NodeStatus.CANCELED,
        NodeStatus.INPUT_REQUIRED,
    }
    while current not in settled:
        if not node.a2a_task_id:
            return current
        task = await ctx.remote.get_task(node.agent_url, node.a2a_task_id)
        if task is not None:
            mapped = _REMOTE_STATE_MAP.get(task.status.state, current)
            if task.artifacts and mapped == NodeStatus.COMPLETED:
                text = " ".join(
                    join_text(artifact.parts)
                    for artifact in task.artifacts
                    if join_text(artifact.parts)
                ).strip()
                if text:
                    node.output = text
            if mapped == NodeStatus.INPUT_REQUIRED and task.status.HasField("message"):
                node.question = join_text(task.status.message.parts)
            current = mapped
            if current in settled:
                return current
        try:
            chunks = ctx.remote.subscribe_task(node.agent_url, node.a2a_task_id)
            current = await consume_chunks(ctx, node, chunks)
        except Exception as exc:
            logger.debug(
                "Follow subscribe failed", context_id=ctx.context_id, node_id=node.id, error=exc
            )
            await asyncio.sleep(0.2)
    return current


async def consume_chunks(
    ctx: OrchestrationContext,
    node: NodeState,
    chunks: AsyncIterator[StreamResponse],
) -> NodeStatus:
    artifacts: list[dict[str, str]] = []
    current = NodeStatus.WORKING
    async for chunk in chunks:
        if chunk.HasField("task"):
            task = chunk.task
            node.a2a_task_id = task.id or node.a2a_task_id
            mapped = _REMOTE_STATE_MAP.get(task.status.state)
            if mapped:
                current = mapped
            if task.artifacts:
                artifacts[:] = [
                    {
                        "id": artifact.artifact_id,
                        "name": artifact.name,
                        "text": join_text(artifact.parts),
                    }
                    for artifact in task.artifacts
                ]
            if current == NodeStatus.SUBMITTED:
                apply_transition(node, NodeStatus.SUBMITTED)
            await emit_state_delta(
                ctx,
                nodes={
                    node.id: {"status": NodeStatus.SUBMITTED, "a2a_task_id": node.a2a_task_id},
                },
            )
        elif chunk.HasField("status_update"):
            remote_state = chunk.status_update.status.state
            mapped = _REMOTE_STATE_MAP.get(remote_state)
            if mapped and mapped != current:
                current = mapped
                if mapped == NodeStatus.INPUT_REQUIRED and chunk.status_update.status.HasField(
                    "message"
                ):
                    msg_text = join_text(chunk.status_update.status.message.parts)
                    node.question = msg_text
                    if msg_text:
                        art = Artifact(
                            artifact_id=uuid.uuid4().hex,
                            name=node.name,
                            parts=[Part(text=msg_text)],
                            metadata=struct(
                                {
                                    "node_id": node.id,
                                    "agent_name": node.agent_name,
                                }
                            ),
                        )
                        await ctx.queue.enqueue_event(
                            TaskArtifactUpdateEvent(
                                task_id=ctx.task_id,
                                context_id=ctx.context_id,
                                artifact=art,
                                append=False,
                                last_chunk=True,
                                metadata=struct(
                                    {
                                        "node_id": node.id,
                                        "agent_name": node.agent_name,
                                    }
                                ),
                            )
                        )
        elif chunk.HasField("artifact_update"):
            update = chunk.artifact_update
            piece = join_text(update.artifact.parts)
            append = bool(update.append)
            entry = next(
                (a for a in artifacts if a["id"] == update.artifact.artifact_id),
                None,
            )
            if append and entry:
                entry["text"] += piece
            else:
                merged = {
                    "id": update.artifact.artifact_id,
                    "name": update.artifact.name,
                    "text": piece,
                }
                if entry:
                    artifacts[artifacts.index(entry)] = merged
                else:
                    artifacts.append(merged)
            art = Artifact(
                artifact_id=update.artifact.artifact_id,
                name=update.artifact.name or node.name,
                parts=[Part(text=piece)],
                metadata=struct(
                    {
                        "node_id": node.id,
                        "agent_name": node.agent_name,
                    }
                ),
            )
            await ctx.queue.enqueue_event(
                TaskArtifactUpdateEvent(
                    task_id=ctx.task_id,
                    context_id=ctx.context_id,
                    artifact=art,
                    append=append,
                    last_chunk=bool(update.last_chunk),
                    metadata=struct(
                        {
                            "node_id": node.id,
                            "agent_name": node.agent_name,
                        }
                    ),
                )
            )
        elif chunk.HasField("message"):
            msg_text = join_text(chunk.message.parts)
            artifacts.append({"id": "message", "name": "message", "text": msg_text})
            art = Artifact(
                artifact_id=uuid.uuid4().hex,
                name=node.name,
                parts=[Part(text=msg_text)],
                metadata=struct(
                    {
                        "node_id": node.id,
                        "agent_name": node.agent_name,
                    }
                ),
            )
            await ctx.queue.enqueue_event(
                TaskArtifactUpdateEvent(
                    task_id=ctx.task_id,
                    context_id=ctx.context_id,
                    artifact=art,
                    append=False,
                    last_chunk=True,
                    metadata=struct(
                        {
                            "node_id": node.id,
                            "agent_name": node.agent_name,
                        }
                    ),
                )
            )

    node.output = (
        " ".join(artifact.get("text", "") for artifact in artifacts if artifact.get("text")).strip()
        or None
    )
    logger.info(
        "consume_chunks done",
        node_id=node.id,
        state=current,
        output_len=len(node.output or ""),
    )
    return current


async def cancel_remote_task(ctx: OrchestrationContext, agent_url: str, task_id: str) -> None:
    await ctx.remote.cancel_task(agent_url, task_id)
