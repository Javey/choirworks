# 设计参考 google-adk sessions/session.py
# （Apache-2.0, Copyright 2026 Google LLC）
from __future__ import annotations

from dataclasses import dataclass, field

from new_choirworks.core.events.event import A2AEvent


@dataclass
class Session:
    """一次会话：事件日志 + 折叠状态。"""

    id: str
    events: list[A2AEvent] = field(default_factory=list)
    state: dict[str, object] = field(default_factory=dict)
