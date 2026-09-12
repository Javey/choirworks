from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TargetIn(BaseModel):
    agent_name: str
    skill_id: str | None = None
    name: str = "single"
    input: dict[str, Any] | None = None


class CreateTaskIn(BaseModel):
    request: str
    target: TargetIn


class RegisterAgentIn(BaseModel):
    name: str
    card_url: str = Field(..., description="A2A agent base URL")
