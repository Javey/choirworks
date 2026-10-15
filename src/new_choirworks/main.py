import asyncio

import uvicorn

from new_choirworks.app.server import create_app
from new_choirworks.config import load_settings


def main() -> None:
    settings = load_settings("config.yaml")
    app = asyncio.run(create_app(settings))
    uvicorn.run(app, host=settings.server.host, port=settings.server.port)


if __name__ == "__main__":
    main()
