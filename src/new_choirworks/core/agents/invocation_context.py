from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from a2a.server.events import EventQueue

if TYPE_CHECKING:
    from new_choirworks.core.events.event import A2AEvent


@dataclass
class InvocationContext:
    """Context for an agent invocation."""

    events: list[A2AEvent] = field(default_factory=list)

    context_id: str | None = None
    task_id: str | None = None
    event_queue: EventQueue | None = None
