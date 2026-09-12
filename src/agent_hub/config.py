from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8080


class LLMConfig(BaseModel):
    planner_model: str = "openai/gpt-4.1"
    assist_model: str = "openai/gpt-4.1-mini"
    timeout_seconds: float = 60.0
    max_plan_retries: int = 2


class SchedulerConfig(BaseModel):
    max_parallel_nodes: int = 5
    node_timeout_seconds: float = 600.0
    max_node_attempts: int = 2
    retry_backoff_seconds: float = 1.0
    replan_on_failure: bool = True
    max_plan_nodes: int = 20


class StoreConfig(BaseModel):
    db_path: Path = Path("./data/agent_hub.db")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AGENT_HUB_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    server: ServerConfig = Field(default_factory=ServerConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)


def load_settings(yaml_path: Path | str | None = None) -> Settings:
    if yaml_path is None:
        return Settings()
    data = yaml.safe_load(Path(yaml_path).read_text(encoding="utf-8")) or {}
    return Settings(**data)
