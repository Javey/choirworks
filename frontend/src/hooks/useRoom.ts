import { useCallback, useEffect, useRef, useState } from "react";

import { api } from "../api/client";
import { subscribeRoomEvents } from "../api/events";
import {
  applyRoomEvent,
  emptyRoom,
  fromRoomSnapshot,
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
  lastSeq.current = view.lastSeq;

  const load = useCallback(async () => {
    if (!conversationId) {
      setView(emptyRoom);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      setView(fromRoomSnapshot(await api.getRoomMessages(conversationId)));
    } catch (exc) {
      setError(messageOf(exc));
    } finally {
      setLoading(false);
    }
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
      await api.postRoomMessage(conversationId, input);
    },
    [conversationId],
  );

  return { view, loading, error, load, send };
}
