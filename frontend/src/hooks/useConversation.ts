import { useCallback, useEffect, useRef, useState } from "react";

import { Role } from "@a2a-js/sdk";

import { getClient } from "../api/a2a-client";
import {
  applyStreamEvent,
  conversationFromTask,
  emptyConversation,
  type ConversationView,
} from "../lib/conversationView";

export interface ConversationSendInput {
  text: string;
  mentions?: string[];
  quote_id?: string;
  interrupt?: boolean;
}

export type ConnectionState = "live" | "reconnecting" | "closed";

export function useConversation(conversationId: string | null) {
  const [view, setView] = useState<ConversationView>(emptyConversation);
  const [connection, setConnection] = useState<ConnectionState>("closed");
  const [error, setError] = useState<string | null>(null);
  const navigateRef = useRef<((id: string) => void) | null>(null);

  const load = useCallback(async () => {
    if (!conversationId) {
      setView(emptyConversation);
      return;
    }
    try {
      const client = await getClient();
      const task = await client.getTask({ id: conversationId, tenant: "" });
      setView(conversationFromTask(task as unknown as Record<string, unknown>, conversationId));
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "加载失败");
    }
  }, [conversationId]);

  useEffect(() => {
    setView(emptyConversation);
    setError(null);
    if (!conversationId) return;
    void load();
  }, [conversationId, load]);

  useEffect(() => {
    if (!conversationId) return;
    let stopped = false;
    let attempt = 0;

    const run = async () => {
      while (!stopped) {
        try {
          const client = await getClient();
          let received = false;
          for await (const event of client.resubscribeTask({ id: conversationId, tenant: "" })) {
            if (stopped) break;
            if (!received) {
              received = true;
              attempt = 0;
              setConnection("live");
            }
            setView((prev) =>
              applyStreamEvent(prev, event as unknown as Record<string, unknown>, Date.now()),
            );
          }
          if (stopped) break;
          throw new Error("流已断开");
        } catch {
          if (stopped) break;
          setConnection("reconnecting");
          await new Promise((r) => setTimeout(r, Math.min(1000 * (attempt + 1), 5000)));
          attempt += 1;
        }
      }
    };
    void run();
    return () => {
      stopped = true;
      setConnection("closed");
    };
  }, [conversationId]);

  const send = useCallback(
    async (input: ConversationSendInput) => {
      const client = await getClient();
      const message = {
        messageId: crypto.randomUUID(),
        role: Role.ROLE_USER,
        parts: [{
          content: { $case: "text" as const, value: input.text },
          metadata: undefined,
          filename: "",
          mediaType: "text/plain",
        }],
        contextId: conversationId ?? "",
        taskId: conversationId ?? "",
        metadata: undefined,
        extensions: [],
        referenceTaskIds: [],
      };

      if (conversationId) {
        await client.sendMessage({ message, tenant: "", configuration: undefined, metadata: undefined });
        return;
      }

      for await (const event of client.sendMessageStream({ message, tenant: "", configuration: undefined, metadata: undefined })) {
        const payload = event.payload;
        if (payload?.$case === "task" && payload.value?.id && navigateRef.current) {
          navigateRef.current(payload.value.id);
          navigateRef.current = null;
        }
        setView((prev) =>
          applyStreamEvent(prev, event as unknown as Record<string, unknown>, Date.now()),
        );
      }
    },
    [conversationId],
  );

  const setOnTaskCreated = useCallback((fn: ((id: string) => void) | null) => {
    navigateRef.current = fn;
  }, []);

  return { view, connection, error, load, send, setOnTaskCreated };
}
