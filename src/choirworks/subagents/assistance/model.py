from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

from pydantic import BaseModel, Field, create_model

from choirworks.orchestration.state import QuestionType


class AssistanceResult(BaseModel):
    """How to handle a blocked agent."""

    target_agent: str | None = None
    instruction: str = ""
    question_type: QuestionType = QuestionType.INPUT
    options: list[str] = Field(default_factory=list)
    multi: bool = False


def assistance_schema(candidate_names: Sequence[str]) -> type[AssistanceResult]:
    """Dynamically constrain ``target_agent`` to the registered candidate agents."""
    if not candidate_names:
        return AssistanceResult
    target = Literal[*candidate_names] | None  # pyright: ignore[reportOperatorIssue]
    return create_model(
        "AssistanceResult",
        __base__=AssistanceResult,
        target_agent=(target, None),
    )
