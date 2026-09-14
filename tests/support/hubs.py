from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import uvicorn

from choirworks.api.app import create_app
from choirworks.config import Settings
from choirworks.sim.ports import free_port


@asynccontextmanager
async def start_hub(
    settings_factory: Callable[[int], Settings],
    llm: Any | None = None,
) -> AsyncIterator[tuple[Any, str]]:
    port = free_port()
    app = await create_app(settings_factory(port), llm=llm)
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - 轮询 uvicorn 启动状态
        await asyncio.sleep(0.02)
    try:
        yield app, f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        with contextlib.suppress(Exception):
            await asyncio.wait_for(task, timeout=5)
