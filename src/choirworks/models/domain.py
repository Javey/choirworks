from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class AgentRecord(BaseModel):
    id: str
    name: str
    card_url: str
    card: dict[str, Any]
    health: str = "unknown"
    last_seen: datetime | None = None
    created_at: datetime
