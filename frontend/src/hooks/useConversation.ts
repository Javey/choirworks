import { useCallback, useEffect, useRef, useState } from "react";

import {
  A2A_URL,
  rpcRequest,
  postJson,
  postSse,
  sendMessageParams,
  type ConnectionState,
} from "../api/a2a";
import {
  applyStreamEvent,
  conversationFromTask,
  emptyConversation,
  type ConversationView,
} from "../lib/conversationView";

type ProtoStruct = Record<string, unknown>;

export interface ConversationSendInput {
  text: string;
  mentions?: string[];
  quote_id?: string;
  interrupt?: boolean;
}

export function useConversation(conversationId: string | null) {
  const [view, setView] = useState<ConversationView>(emptyConversation);
  const [connection, setConnection] = useState<ConnectionState>("closed");
  const [error, setError] = useState<string | null>(null);
  const seqRef = useRef(0);
  const navigateRef = useRef<((id: string) => void) | null>(null);

  const load = useCallback(async () => {
    if (!conversationId) {
      setView(emptyConversation);
      return;
    }
    try {
      const result = await postJson(
        A2A_URL,
        rpcRequest("GetTask", { id: conversationId }),
      );
      const task = (result as ProtoStruct).id ? result : (result as ProtoStruct).task;
      if (task && (task as ProtoStruct).id) {
        setView(conversationFromTask(task as ProtoStruct, conversationId));
      }
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
          let received = false;
          for await (const response of postSse(
            A2A_URL,
            rpcRequest("SubscribeToTask", { id: conversationId }),
          )) {
            if (stopped) break;
            if (response.error) {
              throw new Error(response.error.message ?? "订阅失败");
            }
            if (!received) {
              received = true;
              attempt = 0;
              setConnection("live");
            }
            seqRef.current += 1;
            setView((prev) =>
              applyStreamEvent(prev, response.result ?? {}, seqRef.current),
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
      if (conversationId) {
        // Existing conversation: non-streaming SendMessage, SubscribeToTask delivers events
        await postJson(
          A2A_URL,
          rpcRequest(
            "SendMessage",
            sendMessageParams({
              text: input.text,
              contextId: conversationId,
              taskId: conversationId,
            }),
          ),
        );
        return;
      }
      // New conversation: stream all events, navigate on first Task
      const request = rpcRequest(
        "SendStreamingMessage",
        sendMessageParams({ text: input.text }),
      );
      for await (const response of postSse(A2A_URL, request)) {
        if (response.error) {
          throw new Error(response.error.message ?? "发送失败");
        }
        const result = response.result ?? {};
        const task = (result as ProtoStruct).task as ProtoStruct | undefined;
        if (task && task.id && navigateRef.current) {
          navigateRef.current(String(task.id));
          navigateRef.current = null;
        }
        seqRef.current += 1;
        setView((prev) => applyStreamEvent(prev, result, seqRef.current));
      }
    },
    [conversationId],
  );

  const setOnTaskCreated = useCallback((fn: ((id: string) => void) | null) => {
    navigateRef.current = fn;
  }, []);

  return { view, connection, error, load, send, setOnTaskCreated };
}
