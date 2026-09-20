from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8567


class LLMConfig(BaseModel):
    planner_model: str = "openai/gpt-4.1"
    api_base: str | None = None
    timeout_seconds: float = 60.0
    max_plan_retries: int = 2
    context_window: int | None = None
    compaction_threshold: float = 0.8
    compaction_retention: int = 10


class SchedulerConfig(BaseModel):
    max_parallel_nodes: int = 5
    node_timeout_seconds: float = 600.0
    max_node_attempts: int = 2
    retry_backoff_seconds: float = 1.0
    replan_on_failure: bool = True
    max_revisions: int = 3
    max_plan_nodes: int = 20


class StoreConfig(BaseModel):
    db_path: Path = Path("./data/choirworks.db")


class RecoveryConfig(BaseModel):
    replay_on_startup: bool = True


class SimConfig(BaseModel):
    start_agents: bool = False
    agents: list[dict[str, str]] = Field(default_factory=list)
    chunk_size: int = 2
    chunk_delay: float = 0.04


class A2AConfig(BaseModel):
    public_url: str = "http://127.0.0.1:8567"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CHOIRWORKS_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    server: ServerConfig = Field(default_factory=ServerConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    recovery: RecoveryConfig = Field(default_factory=RecoveryConfig)
    a2a: A2AConfig = Field(default_factory=A2AConfig)
    sim: SimConfig = Field(default_factory=SimConfig)


def load_settings(yaml_path: Path | str | None = None) -> Settings:
    if yaml_path is None:
        return Settings()
    data = yaml.safe_load(Path(yaml_path).read_text(encoding="utf-8")) or {}
    return Settings(**data)
