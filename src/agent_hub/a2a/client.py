from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
from a2a.client import A2ACardResolver, Client, ClientConfig, create_client
from a2a.helpers import new_text_message
from a2a.types import AgentCard, Message, Role, SendMessageRequest, StreamResponse
from google.protobuf.json_format import MessageToDict, ParseDict


class RemoteAgentClient:
    """A2A 协议访问的唯一入口，便于 SDK 升级时集中修改。"""

    def __init__(self, httpx_client: httpx.AsyncClient | None = None):
        self._owns_http = httpx_client is None
        self._http = httpx_client or httpx.AsyncClient()
        self._clients: dict[str, Client] = {}
        self._cards: dict[str, AgentCard] = {}

    async def resolve_card(self, base_url: str) -> AgentCard:
        resolver = A2ACardResolver(httpx_client=self._http, base_url=base_url)
        return await resolver.get_agent_card()

    @staticmethod
    def card_to_dict(card: AgentCard) -> dict[str, Any]:
        return MessageToDict(card)

    @staticmethod
    def card_from_dict(data: dict[str, Any]) -> AgentCard:
        return ParseDict(data, AgentCard())

    async def _client_for(self, agent_url: str) -> Client:
        if agent_url not in self._clients:
            card = await self.resolve_card(agent_url)
            self._cards[agent_url] = card
            self._clients[agent_url] = await create_client(
                agent=card,
                client_config=ClientConfig(streaming=True, httpx_client=self._http),
            )
        return self._clients[agent_url]

    async def send_text(
        self,
        agent_url: str,
        text: str,
        *,
        task_id: str | None = None,
        context_id: str | None = None,
        message_id: str | None = None,
    ) -> AsyncIterator[StreamResponse]:
        client = await self._client_for(agent_url)
        message: Message = new_text_message(
            text, role=Role.ROLE_USER, task_id=task_id, context_id=context_id
        )
        if message_id is not None:
            message.message_id = message_id
        request = SendMessageRequest(message=message)
        async for chunk in client.send_message(request):
            yield chunk

    async def close(self) -> None:
        for client in self._clients.values():
            await client.close()
        self._clients.clear()
        if self._owns_http:
            await self._http.aclose()
