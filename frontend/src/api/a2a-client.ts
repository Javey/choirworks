import { ClientFactory, RestTransportFactory } from "@a2a-js/sdk/client";
import type { Client } from "@a2a-js/sdk/client";
import type { AgentCard } from "@a2a-js/sdk";

let clientPromise: Promise<Client> | null = null;

export function getClient(): Promise<Client> {
  if (clientPromise) return clientPromise;
  clientPromise = (async () => {
    const resp = await fetch("/.well-known/agent-card.json");
    const card = (await resp.json()) as AgentCard;

    for (const iface of card.supportedInterfaces ?? []) {
      iface.url = `${window.location.origin}/v1`;
    }

    const factory = new ClientFactory({
      transports: [new RestTransportFactory()],
    });
    return factory.createFromAgentCard(card);
  })();
  return clientPromise;
}
