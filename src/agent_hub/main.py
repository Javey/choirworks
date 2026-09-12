from __future__ import annotations

import os

import uvicorn

from agent_hub.api.app import create_app
from agent_hub.config import load_settings


def main() -> None:
    settings = load_settings(os.environ.get("AGENT_HUB_CONFIG"))
    uvicorn.run(
        create_app(settings),
        host=settings.server.host,
        port=settings.server.port,
    )


if __name__ == "__main__":
    main()
