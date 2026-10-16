from __future__ import annotations

from dataclasses import dataclass

from new_choirworks.core.workflow.node_status import NodeStatus


@dataclass
class NodeState:
    """State of a node in the workflow."""

    status: NodeStatus = NodeStatus.INACTIVE
    input: object | None = None
