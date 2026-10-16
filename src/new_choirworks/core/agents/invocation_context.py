from dataclasses import dataclass, field

from a2a.server.events import EventQueue

from new_choirworks.core.models.llm_request import ChatMessage


@dataclass
class InvocationContext:
    """Context for an agent invocation."""

    messages: list[ChatMessage] = field(default_factory=list)

    context_id: str | None = None
    task_id: str | None = None
    event_queue: EventQueue | None = None
