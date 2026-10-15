# 设计参考 google-adk workflow（Apache-2.0, Copyright 2026 Google LLC）：
# src/google/adk/workflow/_trigger.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Trigger:
    """Represents a trigger for a downstream node.

    Design reference: google-adk Trigger (Apache-2.0, Copyright 2026 Google LLC).
    Simplified: no use_sub_branch, no branch (no branch isolation).
    """

    input: object | None = None
