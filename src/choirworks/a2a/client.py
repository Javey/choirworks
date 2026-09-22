from __future__ import annotations

import logging
from collections.abc import AsyncIterator

import httpx
from a2a.client import A2ACardResolver, Client, ClientConfig, create_client
from a2a.client.errors import AgentCardResolutionError
from a2a.helpers import new_text_message
from a2a.types import (
    AgentCard,
    CancelTaskRequest,
    GetTaskRequest,
    Message,
    Role,
    SendMessageRequest,
    StreamResponse,
    SubscribeToTaskRequest,
    Task,
)
from google.protobuf.json_format import MessageToDict, ParseDict, ParseError
from pydantic import AnyHttpUrl, JsonValue

logger = logging.getLogger(__name__)


class RemoteAgentClient:
    """A2A 协议访问的唯一入口，便于 SDK 升级时集中修改。"""

    def __init__(self, httpx_client: httpx.AsyncClient | None = None):
        self._owns_http = httpx_client is None
        self._http = httpx_client or httpx.AsyncClient()
        self._clients: dict[str, Client] = {}
        self._cards: dict[str, AgentCard] = {}
        self._retired_clients: list[Client] = []

    async def resolve_card(self, base_url: str) -> AgentCard:
        resolver = A2ACardResolver(httpx_client=self._http, base_url=base_url)
        try:
            card = await resolver.get_agent_card()
            if not card.name.strip() or not card.supported_interfaces:
                raise ValueError("Agent Card requires a name and supported interfaces")
            interfaces = [
                interface
                for interface in card.supported_interfaces
                if interface.protocol_binding == "JSONRPC" and interface.protocol_version == "1.0"
            ]
            if not interfaces:
                raise ValueError("Agent Card must support JSONRPC with A2A version 1.0")
            for interface in interfaces:
                AnyHttpUrl(interface.url)
            return card
        except (ValueError, TypeError, AttributeError, ParseError) as exc:
            raise AgentCardResolutionError(f"invalid agent card: {exc}") from exc

    def cache_card(self, agent_url: str, card: AgentCard) -> None:
        self.invalidate(agent_url)
        self._cards[agent_url] = card

    def invalidate(self, agent_url: str) -> None:
        self._cards.pop(agent_url, None)
        client = self._clients.pop(agent_url, None)
        if client is not None:
            # SDK clients share the HTTP pool; closing one here would also
            # interrupt other agents and streams already using the old card.
            self._retired_clients.append(client)

    @staticmethod
    def card_to_dict(card: AgentCard) -> dict[str, JsonValue]:
        return MessageToDict(card)

    @staticmethod
    def card_from_dict(data: dict[str, JsonValue]) -> AgentCard:
        return ParseDict(data, AgentCard())

    async def _client_for(self, agent_url: str) -> Client:
        if agent_url not in self._clients:
            card = self._cards.get(agent_url)
            if card is None:
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
        logger.info(
            "send_text: agent_url=%s text_len=%d task_id=%s",
            agent_url, len(text), task_id or "(none)",
        )
        message: Message = new_text_message(
            text, role=Role.ROLE_USER, task_id=task_id, context_id=context_id
        )
        if message_id is not None:
            message.message_id = message_id
        request = SendMessageRequest(message=message)
        async for chunk in client.send_message(request):
            yield chunk

    async def get_task(self, agent_url: str, remote_task_id: str) -> Task | None:
        client = await self._client_for(agent_url)
        try:
            task = await client.get_task(GetTaskRequest(id=remote_task_id))
            logger.info(
                "get_task: agent_url=%s task_id=%s state=%s",
                agent_url, remote_task_id,
                task.status.state if task else "none",
            )
            return task
        except Exception:  # noqa: BLE001 - 远程任务可能已过期/不存在
            logger.warning(
                "get_task failed: agent_url=%s task_id=%s",
                agent_url, remote_task_id,
            )
            return None

    async def cancel_task(self, agent_url: str, remote_task_id: str) -> None:
        client = await self._client_for(agent_url)
        try:
            await client.cancel_task(CancelTaskRequest(id=remote_task_id))
            logger.info(
                "cancel_task: agent_url=%s task_id=%s",
                agent_url, remote_task_id,
            )
        except Exception:  # noqa: BLE001 - 取消仅为信号，失败不阻塞
            logger.warning(
                "cancel_task failed: agent_url=%s task_id=%s",
                agent_url, remote_task_id,
            )
            return

    async def subscribe_task(
        self, agent_url: str, remote_task_id: str
    ) -> AsyncIterator[StreamResponse]:
        client = await self._client_for(agent_url)
        async for chunk in client.subscribe(SubscribeToTaskRequest(id=remote_task_id)):
            yield chunk

    async def close(self) -> None:
        for client in [*self._clients.values(), *self._retired_clients]:
            await client.close()
        self._clients.clear()
        self._cards.clear()
        self._retired_clients.clear()
        if self._owns_http:
            await self._http.aclose()
