from new_choirworks.core.models.llm_request import ChatMessage
from dataclasses import dataclass, field
from new_choirworks.core.events.event import A2AEvent

@dataclass
class InvocationContext:
    """Context for an agent invocation."""
    messages: list[ChatMessage] = field(default_factory=list)
