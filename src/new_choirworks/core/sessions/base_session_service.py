# 设计参考 google-adk sessions/base_session_service.py
# （Apache-2.0, Copyright 2026 Google LLC）
from __future__ import annotations

import abc

from new_choirworks.core.events.event import A2AEvent, meta
from new_choirworks.core.sessions.session import Session


class BaseSessionService(abc.ABC):
    """会话服务基类（保留 ADK 的 append 调用链）。"""

    @abc.abstractmethod
    async def get_session(self, session_id: str) -> Session | None: ...

    async def append_event(self, session: Session, event: A2AEvent) -> A2AEvent:
        if meta(event).partial:
            return event
        return self._commit_event_to_session(session, event)

    def _commit_event_to_session(self, session: Session, event: A2AEvent) -> A2AEvent:
        self._update_session_state(session, event)
        session.events.append(event)
        return event

    def _update_session_state(self, session: Session, event: A2AEvent) -> None:
        delta = meta(event).actions.state_delta
        if delta:
            session.state.update(delta)
