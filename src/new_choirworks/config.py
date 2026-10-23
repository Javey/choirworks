from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080


class LLMConfig(BaseModel):
    model: str = "openai/glm-5.1"
    api_base: str | None = None


class StoreConfig(BaseModel):
    db_path: Path = Path("./data/choirworks.db")


class A2AConfig(BaseModel):
    public_url: str = "http://localhost:8080"


class AgentConfig(BaseModel):
    name: str
    url: str
    description: str = ""


class OrchestratorConfig(BaseModel):
    agents: list[AgentConfig] = Field(default_factory=list)


class Settings(BaseSettings):
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="CHOIRWORKS_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    server: ServerConfig = Field(default_factory=ServerConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    store: StoreConfig = Field(default_factory=StoreConfig)
    a2a: A2AConfig = Field(default_factory=A2AConfig)
    orchestrator: OrchestratorConfig = Field(default_factory=OrchestratorConfig)


def load_settings(yaml_path: Path | str | None = None) -> Settings:
    if yaml_path is None:
        return Settings()
    data: dict[str, object] = yaml.safe_load(Path(yaml_path).read_text(encoding="utf-8")) or {}
    return Settings.model_validate(data)
