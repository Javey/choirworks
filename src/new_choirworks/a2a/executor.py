from __future__ import annotations

from typing import override

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.types.a2a_pb2 import (
    Task,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)

from new_choirworks.core.agents.invocation_context import InvocationContext
from new_choirworks.core.agents.llm_agent import LLMAgent


class LLMAgentExecutor(AgentExecutor):
    """Bridge LLMAgent to the A2A AgentExecutor interface."""

    def __init__(self, agent: LLMAgent) -> None:
        self._agent: LLMAgent = agent

    @override
    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
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
        ctx = InvocationContext(
            messages=[{"role": "user", "content": user_text}],
            task_id=context.task_id,
            context_id=context.context_id,
            event_queue=event_queue,
        )
        async for event in self._agent.run_async(ctx):
            await event_queue.enqueue_event(event)

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
        await event_queue.enqueue_event(
            TaskStatusUpdateEvent(
                task_id=context.task_id,
                context_id=context.context_id,
                status=TaskStatus(state=TaskState.TASK_STATE_CANCELED),
            )
        )
