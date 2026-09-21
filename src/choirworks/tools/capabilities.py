from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from choirworks.a2a.patch import PatchResult, PlanPatch


@dataclass(frozen=True, slots=True)
class ToolEffects:
    """Side-effect capabilities the orchestrator grants a tool at call time.

    Tools never reach back into the executor; they perform only the effects
    listed here, bound to the session runtime that owns them.
    """

    max_derived_nodes: int
    join_members: Callable[[list[str], str], Awaitable[None]]
    persist: Callable[[], Awaitable[None]]
    apply_patch_locked: Callable[[PlanPatch], Awaitable[PatchResult]]
