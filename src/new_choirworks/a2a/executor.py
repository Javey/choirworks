from __future__ import annotations

import asyncio
from typing import override

from a2a.helpers.proto_helpers import (
    new_message,
    new_text_part,
    new_text_status_update_event,
)
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.types.a2a_pb2 import Role, Task, TaskState, TaskStatus, TaskStatusUpdateEvent

from new_choirworks.core.agents.base_agent import BaseAgent
from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.events.event import add_author, is_partial_event


class BaseAgentExecutor(AgentExecutor):
    """Bridge BaseAgent (LLMAgent / OrchestratorAgent / ...) to the A2A AgentExecutor interface.

    生命周期：
    - execute 把 agent 运行包成 asyncio task（按 task_id 记账）；
    - cancel 真正取消该 task（触发 agent 的 finally 清理，如 OrchestratorAgent
      的 runtime.shutdown），并等待收口后再发 CANCELED 终态；
    - agent 异常转 TASK_STATE_FAILED 终态（保留错误文本），不静默挂起客户端。
    """

    def __init__(self, agent: BaseAgent) -> None:
        self._agent: BaseAgent = agent
        self._active: dict[str, asyncio.Task[None]] = {}

    @override
    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        assert context.task_id is not None
        task = asyncio.create_task(self._run(context, event_queue))
        self._active[context.task_id] = task
        try:
            await task
        except asyncio.CancelledError:
            if task.cancelled():
                # cancel() 路径：子任务被主动取消，execute 正常收口
                return
            # execute 自身被取消（如流消费端断开）：收掉子任务后传播
            if not task.done():
                _ = task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            raise
        finally:
            _ = self._active.pop(context.task_id, None)

    async def _run(self, context: RequestContext, event_queue: EventQueue) -> None:
        assert context.task_id is not None
        assert context.context_id is not None

        user_text = context.get_user_input()

        # 1. Enqueue initial Task (SUBMITTED)
        task = Task(
            id=context.task_id,
            context_id=context.context_id,
            status=TaskStatus(state=TaskState.TASK_STATE_SUBMITTED),
        )
        await event_queue.enqueue_event(task)

        # 2. Transition to WORKING
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=context.task_id,
                context_id=context.context_id,
                status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
            )
        )

        # 3. Build InvocationContext and forward all agent events
        user_event = add_author(
            new_message(parts=[new_text_part(text=user_text)], role=Role.ROLE_USER), "user"
        )
        ctx = InvocationContext(
            events=[user_event],
            task_id=context.task_id,
            context_id=context.context_id,
            event_queue=event_queue,
        )
        try:
            async for event in self._agent.run_async(ctx):
                if not is_partial_event(event):
                    ctx.events.append(event)
                await event_queue.enqueue_event(event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # agent 异常 → FAILED 终态（保留错误文本），不发 COMPLETED（评审 P2-6）
            await event_queue.enqueue_event(
                new_text_status_update_event(
                    task_id=context.task_id,
                    context_id=context.context_id,
                    state=TaskState.TASK_STATE_FAILED,
                    text=f"{type(e).__name__}: {e}",
                )
            )
            return

        # 4. Terminal state
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=context.task_id,
                context_id=context.context_id,
                status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
            )
        )

    @override
    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        assert context.task_id is not None
        assert context.context_id is not None

        # 真正取消正在运行的 execute（触发 agent finally 清理后台任务），
        # 等收口后再发 CANCELED 终态（评审 P1-3 / P2-7）
        task = self._active.get(context.task_id)
        if task is not None and not task.done():
            _ = task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=context.task_id,
                context_id=context.context_id,
                status=TaskStatus(state=TaskState.TASK_STATE_CANCELED),
            )
        )
