from __future__ import annotations

from datetime import datetime

from pydantic import AnyHttpUrl, BaseModel, Field, JsonValue, field_validator


class AgentRegistration(BaseModel):
    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    card_url: str = Field(description="HTTP(S) base URL used for Agent Card discovery")

    @field_validator("name", mode="before")
    @classmethod
    def trim_name(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value

    @field_validator("card_url")
    @classmethod
    def normalize_url(cls, value: str) -> str:
        url = AnyHttpUrl(value)
        if url.username or url.password or url.query is not None or url.fragment is not None:
            raise ValueError("agent base URL must not contain credentials, query or fragment")
        return str(url).rstrip("/")


class AgentRecord(BaseModel):
    id: str
    name: str
    card_url: str
    card: dict[str, JsonValue]
    health: str = "unknown"
    last_seen: datetime | None = None
    created_at: datetime
