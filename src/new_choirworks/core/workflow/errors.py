class GraphValidationError(ValueError):
    """A malformed graph definition."""


class WorkflowError(RuntimeError):
    """Raised when a workflow execution fails."""
