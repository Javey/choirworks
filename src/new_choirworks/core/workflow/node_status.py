from __future__ import annotations

from enum import Enum


class NodeStatus(Enum):
    """The status of a node in the workflow graph."""

    INACTIVE = 0
    PENDING = 1
    RUNNING = 2
    COMPLETED = 3
    WAITING = 4
    FAILED = 5
    CANCELLED = 6
