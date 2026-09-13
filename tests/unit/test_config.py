from pathlib import Path

from choirworks.config import Settings, load_settings


def test_defaults():
    settings = Settings()
    assert settings.server.host == "127.0.0.1"
    assert settings.server.port == 8080
    assert settings.scheduler.node_timeout_seconds == 600.0
    assert settings.store.db_path == Path("./data/choirworks.db")


def test_env_override(monkeypatch):
    monkeypatch.setenv("CHOIRWORKS_SERVER__PORT", "9999")
    monkeypatch.setenv("CHOIRWORKS_STORE__DB_PATH", "/tmp/choirworks_test.db")
    settings = Settings()
    assert settings.server.port == 9999
    assert str(settings.store.db_path) == "/tmp/choirworks_test.db"


def test_yaml_load(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "server:\n  port: 7777\nscheduler:\n  max_parallel_nodes: 3\n",
        encoding="utf-8",
    )
    settings = load_settings(config_file)
    assert settings.server.port == 7777
    assert settings.scheduler.max_parallel_nodes == 3


def test_llm_and_scheduler_defaults():
    settings = Settings()
    assert settings.llm.planner_model
    assert settings.llm.max_plan_retries == 2
    assert settings.scheduler.max_plan_nodes == 20
    assert settings.scheduler.retry_backoff_seconds == 1.0
    assert settings.scheduler.replan_on_failure is True
