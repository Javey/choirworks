import pytest

from choirworks.config import PolicyConfig, PolicyOverride
from choirworks.core.policy import POLICY_VALUES, PolicyEngine


def engine() -> PolicyEngine:
    return PolicyEngine(
        PolicyConfig(
            default="auto_llm",
            overrides=[
                PolicyOverride(agent_name="payment", policy="human"),
                PolicyOverride(skill_id="deploy_prod", policy="human"),
            ],
        )
    )


def test_node_override_wins():
    assert (
        engine().resolve(node_override="peer_agent", agent_name="payment", skill_id=None)
        == "peer_agent"
    )


def test_agent_override():
    assert engine().resolve(node_override=None, agent_name="payment", skill_id=None) == "human"


def test_skill_override():
    assert engine().resolve(None, "x", "deploy_prod") == "human"


def test_task_policy_then_default():
    assert engine().resolve(None, "x", None, task_policy="human") == "human"
    assert engine().resolve(None, "x", None) == "auto_llm"


def test_invalid_policy_rejected():
    with pytest.raises(ValueError):
        engine().resolve(node_override="banana", agent_name="x", skill_id=None)


def test_policy_values_exported():
    assert POLICY_VALUES == ("auto_llm", "peer_agent", "human")
