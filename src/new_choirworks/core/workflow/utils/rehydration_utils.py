# 设计参考 google-adk workflow/utils/_rehydration_utils.py
# （Apache-2.0, Copyright 2026 Google LLC）
from __future__ import annotations

from dataclasses import dataclass

from new_choirworks.core.events.event import A2AEvent, extract_text, meta
from new_choirworks.core.events.node_path_builder import NodePathBuilder
from new_choirworks.core.workflow.base_node import RouteValue


@dataclass
class ChildScanState:
    """一个节点 run 从事件扫描出的状态（ADK 子集）。"""

    run_id: str | None = None
    output: object | None = None
    route: RouteValue | None = None
    branch: str | None = None
    isolation_scope: str | None = None


def reconstruct_node_states(
    events: list[A2AEvent],
    base_path: str,
    *,
    group_by_direct_child: bool = False,
) -> dict[str, ChildScanState]:
    """Scan events to reconstruct node states under ``base_path``.

    - ``group_by_direct_child=False``：只重建 ``base_path`` 自身状态；
    - ``group_by_direct_child=True``：按直接子段分组，重建每个直接子节点
      的状态（如 ``a@1`` / ``b@2``）。

    输出可来自显式 ``output``、``message_as_output`` 的内容，或通过
    ``node_info.output_for`` 从后代事件委托。
    """
    base = NodePathBuilder.from_string(base_path)
    states: dict[str, ChildScanState] = {}

    for event in events:
        path = NodePathBuilder.from_string(meta(event).node_info.path)
        owner_key = _owner_key(path, base_path, base, group_by_direct_child)
        if owner_key is None:
            continue

        state = states.setdefault(
            owner_key,
            ChildScanState(run_id=NodePathBuilder.from_string(owner_key).run_id),
        )
        if meta(event).partial:
            continue

        is_direct = _is_direct(path, base_path, base, group_by_direct_child)

        output = meta(event).output
        message_as_output = meta(event).node_info.message_as_output
        text = extract_text(event) if message_as_output else ""
        has_output = output is not None or bool(text)

        is_delegated = False
        output_for = meta(event).node_info.output_for
        if has_output and output_for:
            if group_by_direct_child:
                owner_full_path = str(base.append(owner_key))
                is_delegated = owner_full_path in output_for
            else:
                is_delegated = base_path in output_for

        if not (is_direct or is_delegated):
            continue

        if output is not None:
            state.output = output
            state.branch = meta(event).branch
        elif text:
            state.output = text
            state.branch = meta(event).branch

        route = meta(event).actions.route
        if route is not None:
            state.route = route

        if is_direct:
            scope = meta(event).isolation_scope
            if scope is not None:
                state.isolation_scope = scope

    return states


def is_terminal_event(event: A2AEvent) -> bool:
    """Whether ``event`` represents a terminal node outcome.

    当前支持：显式 output、message_as_output 的文本、route。
    暂不支持 error_code / interrupts / long-running。
    """
    if meta(event).partial:
        return False
    if meta(event).output is not None:
        return True
    node_info = meta(event).node_info
    if node_info.message_as_output and extract_text(event):
        return True
    return meta(event).actions.route is not None


def _owner_key(
    path: NodePathBuilder,
    base_path: str,
    base: NodePathBuilder,
    group_by_direct_child: bool,
) -> str | None:
    if not group_by_direct_child:
        if str(path) == base_path or path.is_descendant_of(base):
            return base_path
        return None
    if not path.is_descendant_of(base):
        return None
    return base.get_direct_child(path).leaf_segment


def _is_direct(
    path: NodePathBuilder,
    base_path: str,
    base: NodePathBuilder,
    group_by_direct_child: bool,
) -> bool:
    if not group_by_direct_child:
        return str(path) == base_path
    return path.is_direct_child_of(base)
