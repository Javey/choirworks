from __future__ import annotations

from choirworks.config import PolicyConfig

POLICY_VALUES = ("auto_llm", "peer_agent", "human")


class PolicyEngine:
    def __init__(self, config: PolicyConfig):
        self._config = config
        self._validate(config.default)
        for override in config.overrides:
            self._validate(override.policy)
            if override.agent_name is None and override.skill_id is None:
                raise ValueError("policy override requires agent_name or skill_id")

    def resolve(
        self,
        node_override: str | None,
        agent_name: str | None,
        skill_id: str | None,
        task_policy: str | None = None,
    ) -> str:
        if node_override is not None:
            self._validate(node_override)
            return node_override
        for override in self._config.overrides:
            if override.agent_name is not None and override.agent_name == agent_name:
                return override.policy
            if override.skill_id is not None and override.skill_id == skill_id:
                return override.policy
        if task_policy is not None:
            self._validate(task_policy)
            return task_policy
        return self._config.default

    @property
    def config(self) -> PolicyConfig:
        return self._config

    @staticmethod
    def _validate(policy: str, allowed: tuple[str, ...] = POLICY_VALUES) -> None:
        if policy not in allowed:
            raise ValueError(f"invalid policy: {policy}")
