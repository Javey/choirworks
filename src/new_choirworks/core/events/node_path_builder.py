# 设计参考 google-adk events/_node_path_builder.py
# （Apache-2.0, Copyright 2026 Google LLC）
from __future__ import annotations

from typing import override


class NodePathBuilder:
    """节点在层级 workflow 中的路径（ADK 同构）。

    路径由若干段组成，每段形如 ``node_name@run_id`` 或 ``node_name``。
    例如 ``wf@1/node_a@2``：``wf`` 第 1 轮，其子节点 ``node_a`` 第 2 轮。
    """

    _segments: list[str]

    def __init__(self, segments: list[str]) -> None:
        self._segments = segments

    @classmethod
    def from_string(cls, path_str: str) -> NodePathBuilder:
        if not path_str:
            return cls([])
        return cls(path_str.split("/"))

    @override
    def __str__(self) -> str:
        return "/".join(self._segments)

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, NodePathBuilder):
            return False
        return self._segments == other._segments

    @property
    def node_name(self) -> str:
        """叶子段的节点名（去掉 @run_id）。"""
        if not self._segments:
            return ""
        return self._segments[-1].rsplit("@", 1)[0]

    @property
    def static_path(self) -> str:
        """所有段都去掉 @run_id：``wf@1/node@2`` -> ``wf/node``。"""
        return "/".join(segment.rsplit("@", 1)[0] for segment in self._segments)

    @property
    def leaf_segment(self) -> str:
        """叶子段原样（含 @run_id）。"""
        if not self._segments:
            return ""
        return self._segments[-1]

    @property
    def run_id(self) -> str | None:
        """叶子段的 run_id，没有则 None。"""
        if not self._segments:
            return None
        parts = self._segments[-1].rsplit("@", 1)
        return parts[1] if len(parts) > 1 else None

    @property
    def parent(self) -> NodePathBuilder | None:
        """父路径；根路径返回 None。"""
        if len(self._segments) <= 1:
            return None
        return NodePathBuilder(self._segments[:-1])

    def append(self, node_name: str, run_id: str | None = None) -> NodePathBuilder:
        """返回追加子段后的新路径（不修改自身）。"""
        segment = node_name
        if run_id:
            segment = f"{node_name}@{run_id}"
        return NodePathBuilder(self._segments + [segment])

    def is_descendant_of(self, ancestor: NodePathBuilder) -> bool:
        """是否是 ancestor 的严格后代（不包含相等）。"""
        if len(self._segments) <= len(ancestor._segments):
            return False
        return self._segments[: len(ancestor._segments)] == ancestor._segments

    def is_direct_child_of(self, parent: NodePathBuilder) -> bool:
        """是否是 parent 的直接子节点。"""
        if len(self._segments) != len(parent._segments) + 1:
            return False
        return self._segments[:-1] == parent._segments

    def includes_node_path(self, node_path: str | None) -> bool:
        """事件路径为空、等于自身、或在自身子树内，都算包含。"""
        if not node_path:
            return True
        path = NodePathBuilder.from_string(node_path)
        return path == self or path.is_descendant_of(self)

    def get_direct_child(self, descendant: NodePathBuilder) -> NodePathBuilder:
        """从当前路径通向 descendant 的第一个子段。"""
        if len(descendant._segments) <= len(self._segments):
            raise ValueError("Descendant path is not longer than self path")
        if descendant._segments[: len(self._segments)] != self._segments:
            raise ValueError("Descendant path does not start with self path")
        return NodePathBuilder(descendant._segments[: len(self._segments) + 1])
