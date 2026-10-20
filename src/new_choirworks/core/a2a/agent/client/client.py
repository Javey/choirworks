from dataclasses import dataclass

import httpx


@dataclass
class ClientConfig:
    streaming: bool = False
    httpx_client: httpx.AsyncClient | None = None
