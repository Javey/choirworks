from __future__ import annotations

from pydantic import BaseModel


class AgentInfo(BaseModel):
    """注册到编排平台的远端 A2A agent 目录项。"""

    name: str
    url: str
    description: str = ""
