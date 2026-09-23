from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import structlog
from a2a.server.events import EventQueue

from choirworks.orchestration.events import emit_event
from choirworks.orchestration.state import (
    STATE_JSON_KEY,
    NodeState,
    OrchestrationState,
    state_from_json,
    state_to_json,
)
from choirworks.store.contexts import ContextStore

if TYPE_CHECKING:
    from choirworks.orchestration.context import OrchestrationContext

logger = structlog.get_logger(__name__)


@dataclass
class SessionRuntime:
    context_id: str
    state: OrchestrationState
    lock: asyncio.Lock
    task_id: str
    queue: EventQueue
    runner: asyncio.Task[None] | None = None
    node_tasks: dict[asyncio.Task[None], NodeState] = field(default_factory=dict)
    runner_start_requested: bool = False


class SessionManager:
    """Session lifecycle: load / ensure / persist / evict.

    Owns the in-memory session dict and the context store.  Other components
    receive a :class:`SessionRuntime` via :class:`OrchestrationContext`.
    """

    def __init__(
        self,
        context_store: ContextStore,
    ):
        self._context_store = context_store
        self._sessions: dict[str, SessionRuntime] = {}
        self._lock = asyncio.Lock()

    @property
    def sessions(self) -> dict[str, SessionRuntime]:
        return self._sessions

    async def load_state(self, context_id: str) -> OrchestrationState | None:
        record = await self._context_store.get(context_id)
        if record is None:
            return None
        try:
            return state_from_json(record.state)
        except (ValueError, TypeError):
            logger.warning("Invalid context state", context_id=context_id)
            return None

    async def ensure_session(
        self,
        context_id: str,
        task_id: str,
        event_queue: EventQueue,
    ) -> SessionRuntime:
        async with self._lock:
            runtime = self._sessions.get(context_id)
            if runtime is None:
                state = await self.load_state(context_id)
                runtime = SessionRuntime(
                    context_id=context_id,
                    state=state or OrchestrationState(),
                    lock=asyncio.Lock(),
                    task_id=task_id,
                    queue=event_queue,
                )
                self._sessions[context_id] = runtime
                logger.info(
                    "ensure_session: created",
                    context_id=context_id,
                    restored=state is not None,
                )
        runtime.task_id = task_id
        runtime.queue = event_queue
        return runtime

    def evict_session(self, context_id: str) -> None:
        runtime = self._sessions.pop(context_id, None)
        if runtime is not None:
            runtime.runner = None
            runtime.node_tasks.clear()
            logger.info("evict_session", context_id=context_id)

    def session_is_active(self, context_id: str) -> bool:
        runtime = self._sessions.get(context_id)
        return runtime is not None and runtime.runner is not None and not runtime.runner.done()

    def drop_session(self, context_id: str) -> None:
        self.evict_session(context_id)

    async def persist(self, ctx: OrchestrationContext) -> None:
        runtime = ctx.runtime
        snapshot = state_to_json(runtime.state)
        if self._context_store is not None:
            await self._context_store.upsert_state(runtime.context_id, snapshot)
        metadata: dict[str, object] = {STATE_JSON_KEY: snapshot}
        await emit_event(ctx, "state.updated", metadata=metadata)
