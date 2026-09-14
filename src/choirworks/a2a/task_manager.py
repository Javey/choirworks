from __future__ import annotations

import logging
from typing import Any

from a2a.server.tasks.task_manager import TaskManager
from a2a.types.a2a_pb2 import TaskArtifactUpdateEvent

logger = logging.getLogger(__name__)


class ChoirWorksTaskManager(TaskManager):
    """Custom TaskManager that skips persistence for streaming artifact chunks.

    TaskArtifactUpdateEvent with last_chunk=False are transient streaming
    fragments (typewriter effect). They are forwarded to subscribers but
    NOT persisted to the TaskStore. Only final chunks (last_chunk=True)
    and other event types are saved.
    """

    async def process(self, event: Any) -> Any:
        if isinstance(event, TaskArtifactUpdateEvent) and not event.last_chunk:
            logger.debug(
                "Skipping persistence for streaming artifact chunk "
                "(artifact_id=%s, last_chunk=False)",
                event.artifact.artifact_id,
            )
            return event
        return await super().process(event)
