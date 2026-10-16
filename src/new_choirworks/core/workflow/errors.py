class GraphValidationError(ValueError):
    """A malformed graph definition."""


class WorkflowError(RuntimeError):
    """Raised when a workflow execution fails."""


class NodeTimeoutError(Exception):
    """Raised when a node exceeds its configured timeout."""

    def __init__(self, *, node_name: str, timeout: float) -> None:
        self.node_name: str = node_name
        self.timeout: float = timeout
        super().__init__(f"Node '{node_name}' timed out after {timeout} seconds.")
