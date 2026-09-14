import { useCallback, useRef, useState } from "react";

import { Role } from "@a2a-js/sdk";

import { getClient } from "../api/a2a-client";
import {
  applyStreamEvent,
  emptyConversation,
  type ConversationView,
} from "../lib/conversationView";

export interface ConversationSendInput {
  text: string;
  mentions?: string[];
  quote_id?: string;
  interrupt?: boolean;
}

export function useConversation(conversationId: string | null) {
  const [view, setView] = useState<ConversationView>(emptyConversation);
  const [error] = useState<string | null>(null);
  const navigateRef = useRef<((id: string) => void) | null>(null);

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

  return { view, error, send, setOnTaskCreated };
}
