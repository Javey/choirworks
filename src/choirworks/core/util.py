from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel

MAX_METADATA_OUTPUT = 2000


def now_iso() -> str:
    """Current UTC timestamp in ISO format."""
    return datetime.now(UTC).isoformat()


def truncate(
    text: str | None, limit: int = MAX_METADATA_OUTPUT
) -> str | None:
    """Truncate text to *limit* characters, returning None for None input."""
    if text is None:
        return None
    if len(text) <= limit:
        return text
    return text[:limit]


def as_model[T: BaseModel](item: object, model: type[T]) -> T:
    """Extract a typed model from a ToolCallResult, validating if needed."""
    args = getattr(item, "args", item)
    if isinstance(args, model):
        return args
    if isinstance(args, BaseModel):
        return model.model_validate(args.model_dump())
    return model.model_validate(args)
