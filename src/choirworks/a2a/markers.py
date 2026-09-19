from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

MarkerIntent = Literal["deliver", "need_info", "revise"]

_MARKER_RE = re.compile(
    r"^\[cw:(deliver|need_info|assist|revise)\][ \t]*(.*)$",
    re.MULTILINE,
)
_INTENT_MAP: dict[str, MarkerIntent] = {
    "deliver": "deliver",
    "need_info": "need_info",
    "assist": "need_info",
    "revise": "revise",
}


@dataclass(frozen=True)
class Marker:
    intent: MarkerIntent
    text: str = ""


def parse_marker(output: str | None) -> Marker | None:
    """Parse a receipt marker from the first non-empty line of an output.

    Only the first non-empty line is considered so that an agent echoing the
    dispatch text (which contains marker examples) cannot trigger a marker.
    """
    if not output:
        return None
    for line in output.splitlines():
        if not line.strip():
            continue
        match = _MARKER_RE.match(line)
        if match is None:
            return None
        return Marker(
            intent=_INTENT_MAP[match.group(1)],
            text=match.group(2).strip(),
        )
    return None
