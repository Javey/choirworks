from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from choirworks.orchestration.planning.patch import PlanPatch


class OutcomeResult(BaseModel):
    """What an agent's final reply means for the plan."""

    intent: Literal["deliver", "need_info", "revise"]
    question: str = ""
    patch: PlanPatch | None = None
