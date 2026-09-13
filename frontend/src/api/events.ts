import type { EventDto } from "../lib/types";

export type ConnectionState = "live" | "reconnecting" | "closed";

export const ROOM_EVENT_TYPES = [
  "message.posted",
  "message.delivered",
  "room.participant_joined",
  "room.summary_updated",
] as const;

const EVENT_TYPES = [
  "task.created",
  "task.state_changed",
  "task.completed",
  "task.failed",
  "plan.created",
  "plan.extended",
  "plan.superseded",
  "node.dispatch.intent",
  "node.dispatched",
  "node.state_changed",
  "node.artifact",
  "node.output",
  "node.retry.scheduled",
  "node.invalidated",
  "node.cancel.sent",
  "intervention.requested",
  "intervention.resolved",
  "intervention.failed",
  "checkpoint.created",
  "rollback.performed",
  "error",
] as const;

export interface TaskStreamHandlers {
  onEvent: (event: EventDto) => void;
  onState: (state: ConnectionState) => void;
}

export function subscribeTaskEvents(
  taskId: string,
  afterSeq: number,
  handlers: TaskStreamHandlers,
): () => void {
  const source = new EventSource(`/v1/tasks/${taskId}/events?after_seq=${afterSeq}`);
  const listeners = EVENT_TYPES.map((type) => {
    const listener = (message: MessageEvent<string>) => {
      let payload: Record<string, unknown> = {};
      try {
        payload = JSON.parse(message.data) as Record<string, unknown>;
      } catch {
        payload = {};
      }
      const seq = Number(message.lastEventId);
      handlers.onEvent({
        seq: Number.isFinite(seq) && seq > 0 ? seq : afterSeq,
        type,
        payload,
      });
    };
    source.addEventListener(type, listener as EventListener);
    return [type, listener] as const;
  });

  source.onopen = () => handlers.onState("live");
  source.onerror = () => {
    handlers.onState(source.readyState === EventSource.CLOSED ? "closed" : "reconnecting");
  };

  return () => {
    for (const [type, listener] of listeners) {
      source.removeEventListener(type, listener as EventListener);
    }
    source.close();
    handlers.onState("closed");
  };
}

export function subscribeRoomEvents(
  conversationId: string,
  afterSeq: number,
  handlers: TaskStreamHandlers,
): () => void {
  const source = new EventSource(
    `/v1/conversations/${conversationId}/stream?since_seq=${afterSeq}`,
  );
  const listeners = ROOM_EVENT_TYPES.map((type) => {
    const listener = (message: MessageEvent<string>) => {
      let payload: Record<string, unknown> = {};
      try {
        payload = JSON.parse(message.data) as Record<string, unknown>;
      } catch {
        payload = {};
      }
      const seq = Number(message.lastEventId);
      handlers.onEvent({
        seq: Number.isFinite(seq) && seq > 0 ? seq : afterSeq,
        type,
        payload,
      });
    };
    source.addEventListener(type, listener as EventListener);
    return [type, listener] as const;
  });

  source.onopen = () => handlers.onState("live");
  source.onerror = () => {
    handlers.onState(
      source.readyState === EventSource.CLOSED ? "closed" : "reconnecting",
    );
  };

  return () => {
    for (const [type, listener] of listeners) {
      source.removeEventListener(type, listener as EventListener);
    }
    source.close();
    handlers.onState("closed");
  };
}
