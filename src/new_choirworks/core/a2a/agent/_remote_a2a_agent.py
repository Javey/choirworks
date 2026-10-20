from collections.abc import AsyncGenerator
from typing import override
from urllib.parse import urlparse

import httpx
from a2a.client import (
    A2ACardResolver,
)
from a2a.client import (
    Client as A2AClient,
)
from a2a.client import (
    ClientConfig as A2AClientConfig,
)
from a2a.client import (
    ClientFactory as A2AClientFactory,
)
from a2a.helpers.proto_helpers import new_text_status_update_event
from a2a.types import AgentCard
from a2a.types.a2a_pb2 import TaskState
from pydantic import PrivateAttr

from new_choirworks.core.agents.base_agent import BaseAgent
from new_choirworks.core.events.event import A2AEvent, is_final_artifact
from new_choirworks.core.workflow.context import Context, InvocationContext

DEFAULT_TIMEOUT = 600.0


class AgentCardResolutionError(Exception):
    pass


class RemoteA2aAgent(BaseAgent):
    agent_url: str
    httpx_client: httpx.AsyncClient | None = None
    a2a_client_factory: A2AClientFactory | None = None
    timeout: float | None = DEFAULT_TIMEOUT

    _agent_card: AgentCard | None = PrivateAttr(default=None)
    _a2a_client: A2AClient | None = PrivateAttr(default=None)
    _is_resolved: bool = PrivateAttr(default=False)
    _httpx_client_needs_cleanup: bool = PrivateAttr(default=False)

    @override
    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[A2AEvent, None]:
        assert ctx.task_id
        assert ctx.context_id

        try:
            a2a_client = await self._ensure_resolved(ctx)
        except Exception as e:
            yield new_text_status_update_event(
                task_id=ctx.task_id,
                context_id=ctx.context_id,
                state=TaskState.TASK_STATE_FAILED,
                text=f"Failed to initialize remote A2A agent {self.name} - {e}",
            )
            return

        # message = self.

    @override
    async def _run_impl(
        self, ctx: Context, node_input: object | None = None
    ) -> AsyncGenerator[A2AEvent, None]:
        promoted = False
        async for event in super()._run_impl(ctx, node_input):
            if not promoted and self._promote_response_to_output(event):
                promoted = True
            yield event

    def _promote_response_to_output(self, event: A2AEvent) -> bool:
        if not is_final_artifact(event):
            return False
        return True

    async def _ensure_resolved(self, ctx: InvocationContext | None) -> A2AClient:
        if self._is_resolved and self._a2a_client:
            return self._a2a_client

        try:
            if not self._agent_card:
                agent_card = await self._resolve_agent_card(ctx)
                self._validate_agent_card(agent_card)
                self._agent_card = agent_card

            if not self.description and self._agent_card.description:
                self.description = self._agent_card.description

            if not self._a2a_client:
                _client = await self._ensure_httpx_client()
                if not self.a2a_client_factory:
                    raise AgentCardResolutionError(
                        f"Failed to initialize A2A client factory for agent {self.name}"
                    )
                self._a2a_client = self.a2a_client_factory.create(self._agent_card)

            self._is_resolved = True

            return self._a2a_client
        except Exception as e:
            raise AgentCardResolutionError(
                f"Failed to initialize remote A2A agent {self.name} - {e}"
            ) from e

    async def _resolve_agent_card(self, _ctx: InvocationContext | None) -> AgentCard:
        return await self._resolve_agent_card_from_url(self.agent_url)

    async def _resolve_agent_card_from_url(self, url: str) -> AgentCard:
        try:
            parsed_url = urlparse(url)
            if not parsed_url.scheme or not parsed_url.netloc:
                raise ValueError(f"Invalid URL: {url}")

            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            relative_card_path = parsed_url.path

            httpx_client = await self._ensure_httpx_client()
            resolver = A2ACardResolver(httpx_client=httpx_client, base_url=base_url)
            http_kwargs = {}
            if self._httpx_client_needs_cleanup:
                http_kwargs = {"timeout": self.timeout}

            return await resolver.get_agent_card(
                relative_card_path=relative_card_path,
                http_kwargs=http_kwargs,
            )
        except Exception as e:
            raise AgentCardResolutionError(
                f"Failed to resolve agent card from URL: {url} - {e}"
            ) from e

    def _validate_agent_card(self, agent_card: AgentCard) -> None:
        card_url = _get_card_rpc_url(agent_card)
        if not card_url:
            raise AgentCardResolutionError(
                f"Agent card {self.name} does not have a valid RPC communication"
            )

        try:
            parsed_url = urlparse(card_url)
            if not parsed_url.scheme or not parsed_url.netloc:
                raise ValueError(f"Invalid URL: {card_url}")
        except Exception as e:
            raise AgentCardResolutionError(
                f"Invalid RPC URL for agent card {card_url} - error: {e}"
            ) from e

        # TODO: _validate_card_rpc_targets

    async def _ensure_httpx_client(self) -> httpx.AsyncClient:
        if not self.httpx_client:
            self.httpx_client = httpx.AsyncClient(
                timeout=httpx.Timeout(timeout=self.timeout),
            )
            self._httpx_client_needs_cleanup = True

        if not self.a2a_client_factory:
            self.a2a_client_factory = A2AClientFactory(
                config=A2AClientConfig(streaming=True, httpx_client=self.httpx_client),
            )

        return self.httpx_client

    async def cleanup(self) -> None:
        if self._httpx_client_needs_cleanup and self.httpx_client:
            await self.httpx_client.aclose()


def _get_card_rpc_url(card: AgentCard, protocol_binding: str = "jsonrpc") -> str | None:
    interfaces = list(card.supported_interfaces)
    if not interfaces:
        return None
    for iface in interfaces:
        if getattr(iface, "protocol_binding", None) == protocol_binding:
            return iface.url

    return interfaces[0].url
