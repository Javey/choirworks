# 设计参考 google-adk events/（Apache-2.0, Copyright 2026 Google LLC）：
# 仅借鉴「agent 运行时产出统一事件对象」的概念；本仓不引入内部 Event 对象，
# 事件词汇表即 A2A 标准事件（docs/agent-architecture-plan.md 决策 2）。
from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Protocol

import structlog
from a2a.server.events import EventQueue
from a2a.types.a2a_pb2 import (
    Artifact,
    Message,
    Part,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatusUpdateEvent,
)
from google.protobuf import struct_pb2
from google.protobuf.json_format import ParseDict

from choirworks.a2a.wire import status_update

logger = structlog.get_logger(__name__)


class CwType(StrEnum):
    THOUGHT = "thought"
    TEXT = "text"
    FUNCTION_CALL = "function_call"
    QUESTION = "question"
    DECISION = "decision"
    COMPACTION = "compaction"


ChunkKind = Literal[CwType.THOUGHT, CwType.TEXT]

A2AEvent = TaskStatusUpdateEvent | TaskArtifactUpdateEvent | Message


@dataclass
class ResultEvent[T]:
    """agent 的结构化结果——纯内部信号，不上 wire。

    agent 的 ``run_async`` yield 此事件结束一轮调用，调用方
    （``run_agent``）拦截它取回类型化结果，其余 A2A 事件推队列。
    """

    value: T


type AgentEvent = A2AEvent | ResultEvent[object]

A2A_CW_EVENTS_URI = "https://github.com/Javey/choirworks/extensions/cw-events/v1"


class EventSink(Protocol):
    """回合上下文的发布能力：事件经 queue 推出，task/context 定位。"""

    @property
    def queue(self) -> EventQueue: ...

    @property
    def task_id(self) -> str: ...

    @property
    def context_id(self) -> str: ...


async def emit(ctx: EventSink, event: A2AEvent) -> None:
    """单点发布：把一个 A2A 标准事件推上回合队列。"""
    logger.info("emit", task_id=ctx.task_id, a2a_event=type(event).__name__)
    await ctx.queue.enqueue_event(event)


def status_event(
    task_id: str,
    context_id: str,
    state: TaskState,
    *,
    metadata: Mapping[str, object] | None = None,
    message: Message | None = None,
) -> TaskStatusUpdateEvent:
    return status_update(task_id, context_id, state, metadata=metadata, message=message)


def chunk_event(
    task_id: str,
    context_id: str,
    *,
    text: str,
    kind: ChunkKind,
    author: str,
    artifact_id: str,
    append: bool,
    last_chunk: bool,
) -> TaskArtifactUpdateEvent:
    """流式思考/正文分块：``cw_type`` 与 ``author`` 在 part metadata。"""
    part = Part(text=text)
    meta = struct_pb2.Struct()
    meta.update({"cw_type": kind, "author": author})
    part.metadata.CopyFrom(meta)
    return TaskArtifactUpdateEvent(
        task_id=task_id,
        context_id=context_id,
        artifact=Artifact(artifact_id=artifact_id, parts=[part]),
        append=append,
        last_chunk=last_chunk,
    )


def function_call_event(
    task_id: str,
    context_id: str,
    *,
    function_name: str,
    args: Mapping[str, object],
    result: Mapping[str, object],
) -> TaskArtifactUpdateEvent:
    """函数调用回执：payload 在 data part，``cw_type`` 在 part metadata。"""
    data_value = struct_pb2.Value()
    ParseDict(
        {
            "function_name": function_name,
            "function_args": args,
            "function_result": result,
        },
        data_value,
    )
    part_meta = struct_pb2.Struct()
    part_meta.update({"cw_type": CwType.FUNCTION_CALL})
    part = Part()
    part.data.CopyFrom(data_value)
    part.metadata.CopyFrom(part_meta)
    return TaskArtifactUpdateEvent(
        task_id=task_id,
        context_id=context_id,
        artifact=Artifact(artifact_id=uuid.uuid4().hex, parts=[part]),
        append=False,
        last_chunk=True,
    )


def state_delta_event(
    task_id: str,
    context_id: str,
    delta: Mapping[str, object],
    *,
    state: TaskState = TaskState.TASK_STATE_WORKING,
) -> TaskStatusUpdateEvent:
    """计划状态增量：挂在 status 事件 metadata（``cw_delta``）。

    不放 status.message——那是问答卡（question data part）的独占槽位，
    状态事件不带 message 才不会把它顶掉。
    """
    return status_event(task_id, context_id, state, metadata={"cw_delta": dict(delta)})
