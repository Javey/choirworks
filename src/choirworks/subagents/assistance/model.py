from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

from pydantic import BaseModel, Field, create_model

from choirworks.orchestration.state import QuestionType

# Explicit sentinel the model can pick to escalate to a human.  Keeping it in
# the tool enum avoids relying on ``null`` / empty-string encoding, which some
# providers render poorly and which once flipped a "human" intent into an agent.
HUMAN_TARGET = "human"


class AssistanceResult(BaseModel):
    """How to handle a blocked agent.  ``target_agent is None`` means human."""

    target_agent: str | None = None
    instruction: str = ""
    question_type: QuestionType = QuestionType.INPUT
    options: list[str] = Field(default_factory=list)
    multi: bool = False


def assistance_schema(candidate_names: Sequence[str]) -> type[AssistanceResult]:
    """Constrain ``target_agent`` to the candidates plus the human sentinel."""
    if not candidate_names:
        return AssistanceResult
    target = Literal[HUMAN_TARGET, *candidate_names]
    return create_model(
        "AssistanceResult",
        __base__=AssistanceResult,
        target_agent=(target, HUMAN_TARGET),
    )
