import { useCallback, useEffect, useRef, useState } from "react";

import { api } from "../api/client";
import { subscribeRoomEvents } from "../api/events";
import {
  applyRoomEvent,
  emptyRoom,
  fromRoomSnapshot,
  mergeRoomSnapshot,
  type RoomView,
} from "../lib/roomView";
import type { RoomSendInput } from "../components/room/RoomComposer";

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : "请求失败";
}

export function useRoom(conversationId: string | null) {
  const [view, setView] = useState<RoomView>(emptyRoom);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const lastSeq = useRef(0);
  const loadedRoom = useRef<string | null>(null);
  const viewRoom = useRef<string | null>(null);
  lastSeq.current = view.lastSeq;

  const load = useCallback(async () => {
    if (!conversationId) {
      loadedRoom.current = null;
      viewRoom.current = null;
      setView(emptyRoom);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const snapshot = await api.getRoomMessages(conversationId);
      const sameRoom =
        loadedRoom.current === conversationId ||
        viewRoom.current === conversationId;
      loadedRoom.current = conversationId;
      viewRoom.current = conversationId;
      setView((previous) =>
        sameRoom ? mergeRoomSnapshot(previous, snapshot) : fromRoomSnapshot(snapshot),
      );
    } catch (exc) {
      setError(messageOf(exc));
    } finally {
      setLoading(false);
    }
  }, [conversationId]);

  useEffect(() => {
    loadedRoom.current = null;
    viewRoom.current = null;
    setView(emptyRoom);
  }, [conversationId]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (!conversationId) return;
    return subscribeRoomEvents(conversationId, lastSeq.current, {
      onEvent: (event) =>
        setView((previous) => applyRoomEvent(previous, event)),
      onState: () => undefined,
    });
  }, [conversationId, load]);

  const send = useCallback(
    async (input: RoomSendInput) => {
      if (!conversationId) return;
      const posted = await api.postRoomMessage(conversationId, input);
      viewRoom.current = conversationId;
      setView((previous) =>
        applyRoomEvent(previous, {
          seq: posted.seq,
          type: "message.posted",
          payload: {
            message_id: posted.message_id,
            conversation_id: conversationId,
            seq: posted.seq,
            role: "user",
            sender: "CEO",
            text: input.text,
            mentions: input.mentions ?? [],
            quote_id: input.quote_id ?? null,
            task_id: posted.task_id ?? null,
            created_at: new Date().toISOString(),
          },
        }),
      );
    },
    [conversationId],
  );

  return { view, loading, error, load, send };
}
