from __future__ import annotations

from typing import TYPE_CHECKING

from a2a.types.a2a_pb2 import TaskState
from pydantic import BaseModel

if TYPE_CHECKING:
    from choirworks.a2a.executor import ChoirWorksAgentExecutor, SessionRuntime
    from choirworks.tools.base import AgentFunction, FunctionResult


async def emit_function_call(
    executor: ChoirWorksAgentExecutor,
    runtime: SessionRuntime,
    func: AgentFunction,
    args: BaseModel,
    result: FunctionResult,
    *,
    state_name: int = TaskState.TASK_STATE_WORKING,
) -> None:
    """Emit a ``function_call`` wire event on the A2A event queue.

    This replaces the previous pattern of one ``kind`` string per semantic
    event.  The frontend dispatches on ``function_name`` and reads typed
    ``function_args`` / ``function_result`` payloads.
    """
    await executor._emit_event(
        runtime,
        "function_call",
        state_name,
        function_name=func.name,
        function_args=args.model_dump(),
        function_result=result.model_dump(),
    )


async def emit_function_error(
    executor: ChoirWorksAgentExecutor,
    runtime: SessionRuntime,
    func: AgentFunction,
    error: str,
    *,
    state_name: int = TaskState.TASK_STATE_FAILED,
) -> None:
    """Emit a failed ``function_call`` event (e.g. planning failed)."""
    await executor._emit_event(
        runtime,
        "function_call",
        state_name,
        function_name=func.name,
        function_args={},
        function_result={"success": False, "error": error},
    )
