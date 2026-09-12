from pathlib import Path

from agent_hub.config import Settings, load_settings


def test_defaults():
    settings = Settings()
    assert settings.server.host == "127.0.0.1"
    assert settings.server.port == 8080
    assert settings.scheduler.node_timeout_seconds == 600.0
    assert settings.store.db_path == Path("./data/agent_hub.db")


def test_env_override(monkeypatch):
    monkeypatch.setenv("AGENT_HUB_SERVER__PORT", "9999")
    monkeypatch.setenv("AGENT_HUB_STORE__DB_PATH", "/tmp/agent_hub_test.db")
    settings = Settings()
    assert settings.server.port == 9999
    assert str(settings.store.db_path) == "/tmp/agent_hub_test.db"


def test_yaml_load(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "server:\n  port: 7777\nscheduler:\n  max_parallel_nodes: 3\n",
        encoding="utf-8",
    )
    settings = load_settings(config_file)
    assert settings.server.port == 7777
    assert settings.scheduler.max_parallel_nodes == 3
