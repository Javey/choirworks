from __future__ import annotations

from typing import TYPE_CHECKING, Any

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


async def emit_state_delta(
    executor: ChoirWorksAgentExecutor,
    runtime: SessionRuntime,
    *,
    nodes: dict[str, dict[str, Any]] | None = None,
    members: list[dict[str, Any]] | None = None,
    interventions: dict[str, dict[str, Any]] | None = None,
    state_name: int = TaskState.TASK_STATE_WORKING,
) -> None:
    """Emit a ``state_delta`` wire event carrying typed state changes.

    Replaces B-class ``kind`` events (node.completed, intervention.expired,
    room.participant_joined, …) with a single unified delta.  The frontend
    merges the delta into its view and derives notifications from the changes.

    * ``nodes`` — ``{node_id: {status, output?, error?, …}}``; new node ids
      are added to the view.
    * ``members`` — list of new member dicts to append.
    * ``interventions`` — ``{intervention_id: {status, node_id, kind, …}}``;
      the frontend derives lifecycle notifications from status transitions.
    """
    delta: dict[str, Any] = {}
    if nodes:
        delta["nodes"] = nodes
    if members:
        delta["members"] = members
    if interventions:
        delta["interventions"] = interventions
    if not delta:
        return
    await executor._emit_event(
        runtime,
        "state_delta",
        state_name,
        **delta,
    )
