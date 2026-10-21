from typing import cast

from new_choirworks.core.events.event import A2AEvent, EventMeta

A2A_METADATA_PREFIX = "a2a:"
TASK_ID = A2A_METADATA_PREFIX + "task_id"
CONTEXT_ID = A2A_METADATA_PREFIX + "context_id"


class A2AEventMeta(EventMeta):
    @property
    def task_id(self) -> str | None:
        if TASK_ID in self._event.metadata:
            return cast(str, self._event.metadata[TASK_ID])

    @task_id.setter
    def task_id(self, value: str) -> None:
        self._event.metadata[TASK_ID] = value

    @property
    def context_id(self) -> str | None:
        if CONTEXT_ID in self._event.metadata:
            return cast(str, self._event.metadata[CONTEXT_ID])

    @context_id.setter
    def context_id(self, value: str) -> None:
        self._event.metadata[CONTEXT_ID] = value


def a2a_meta(event: A2AEvent) -> A2AEventMeta:
    return A2AEventMeta(event)
