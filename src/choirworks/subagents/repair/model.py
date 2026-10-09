from __future__ import annotations

from pydantic import BaseModel

from choirworks.orchestration.planning.patch import PlanPatch


class RepairResult(BaseModel):
    """An incremental repair patch for a failed plan."""

    patch: PlanPatch
    reasoning: str = ""
