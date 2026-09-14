from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8567
    frontend_dir: Path = Path("frontend/dist")


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
    db_path: Path = Path("./data/choirworks.db")


class PolicyOverride(BaseModel):
    agent_name: str | None = None
    skill_id: str | None = None
    policy: str


class PolicyConfig(BaseModel):
    default: str = "auto_llm"
    on_timeout: str = "escalate"
    timeout_seconds: float = 900.0
    overrides: list[PolicyOverride] = Field(default_factory=list)


class RecoveryConfig(BaseModel):
    reconcile_interval_seconds: float = 30.0
    replay_on_startup: bool = True


class A2AConfig(BaseModel):
    public_url: str = "http://127.0.0.1:8567"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CHOIRWORKS_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    server: ServerConfig = Field(default_factory=ServerConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    policies: PolicyConfig = Field(default_factory=PolicyConfig)
    recovery: RecoveryConfig = Field(default_factory=RecoveryConfig)
    a2a: A2AConfig = Field(default_factory=A2AConfig)


def load_settings(yaml_path: Path | str | None = None) -> Settings:
    if yaml_path is None:
        return Settings()
    data = yaml.safe_load(Path(yaml_path).read_text(encoding="utf-8")) or {}
    return Settings(**data)
