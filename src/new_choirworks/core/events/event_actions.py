from __future__ import annotations

from typing import cast

from google.protobuf import json_format, struct_pb2

from new_choirworks.core.workflow.base_node import RouteValue

EVENT_ACTION_STATE_DELTA = "actions.state_delta"
EVENT_ACTION_ROUTE = "actions.route"


class EventActions:
    """Typed reference view over an event metadata Struct's action fields.

    Design reference: google-adk EventActions (Apache-2.0, Copyright 2026
    Google LLC). Only state_delta + route; other ADK actions not implemented.
    Reference semantics — reads/writes go directly to backing Struct.
    """

    _metadata: struct_pb2.Struct

    def __init__(self, metadata: struct_pb2.Struct) -> None:
        self._metadata = metadata

    @property
    def state_delta(self) -> dict[str, object]:
        if EVENT_ACTION_STATE_DELTA not in self._metadata:
            return {}
        value = cast(struct_pb2.Struct, self._metadata[EVENT_ACTION_STATE_DELTA])
        return json_format.MessageToDict(value, preserving_proto_field_name=True)

    @state_delta.setter
    def state_delta(self, value: dict[str, object]) -> None:
        s = struct_pb2.Struct()
        _ = json_format.ParseDict(value, s)
        self._metadata[EVENT_ACTION_STATE_DELTA] = s

    @property
    def route(self) -> RouteValue | None:
        if EVENT_ACTION_ROUTE not in self._metadata:
            return None
        return cast(RouteValue, self._metadata[EVENT_ACTION_ROUTE])

    @route.setter
    def route(self, value: RouteValue) -> None:
        self._metadata[EVENT_ACTION_ROUTE] = value
