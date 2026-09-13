from __future__ import annotations

import os

import uvicorn

from choirworks.api.app import create_app
from choirworks.config import load_settings


def main() -> None:
    settings = load_settings(os.environ.get("CHOIRWORKS_CONFIG"))
    uvicorn.run(
        create_app(settings),
        host=settings.server.host,
        port=settings.server.port,
    )


if __name__ == "__main__":
    main()
