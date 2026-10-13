from pathlib import Path

import pytest
import yaml
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def llm_kwargs():
    cfg = yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8"))["llm"]
    return {"model": cfg["planner_model"], "base_url": cfg["api_base"]}
