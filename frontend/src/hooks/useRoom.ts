import { useCallback, useEffect, useRef, useState } from "react";

import { sendRoomMessage, subscribeRoom } from "../api/a2a";
import { api } from "../api/client";
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
  const loadedRoom = useRef<string | null>(null);
  const viewRoom = useRef<string | null>(null);

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
    return subscribeRoom(conversationId, {
      onSnapshot: (snapshot) => {
        const sameRoom = viewRoom.current === conversationId;
        loadedRoom.current = conversationId;
        viewRoom.current = conversationId;
        setView((previous) =>
          sameRoom ? mergeRoomSnapshot(previous, snapshot) : fromRoomSnapshot(snapshot),
        );
      },
      onEvent: (event) => setView((previous) => applyRoomEvent(previous, event)),
      onState: () => undefined,
    });
  }, [conversationId]);

  const send = useCallback(
    async (input: RoomSendInput) => {
      if (!conversationId) return;
      const posted = await sendRoomMessage(conversationId, input);
      viewRoom.current = conversationId;
      if (posted) {
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
        return;
      }
      await load();
    },
    [conversationId, load],
  );

  return { view, loading, error, load, send };
}
