from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

import structlog
import uvicorn

from choirworks.api.app import create_app
from choirworks.config import load_settings

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="%H:%M:%S"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    cache_logger_on_first_use=True,
)


def main() -> None:
    config_path = os.environ.get("CHOIRWORKS_CONFIG")
    if config_path is None and Path("config.yaml").exists():
        config_path = "config.yaml"
    settings = load_settings(config_path)
    app = asyncio.run(create_app(settings))
    uvicorn.run(
        app,
        host=settings.server.host,
        port=settings.server.port,
    )


if __name__ == "__main__":
    main()
