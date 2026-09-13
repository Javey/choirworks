import type { EventDto, RoomMessageDto, RoomMessagesDto } from "./types";

export interface RoomView {
  messages: RoomMessageDto[];
  members: RoomMessagesDto["members"];
  summary: RoomMessagesDto["summary"];
  lastSeq: number;
}

export const emptyRoom: RoomView = {
  messages: [],
  members: [],
  summary: null,
  lastSeq: 0,
};

export function fromRoomSnapshot(snapshot: RoomMessagesDto): RoomView {
  return {
    messages: [...snapshot.messages].sort((left, right) => left.seq - right.seq),
    members: snapshot.members,
    summary: snapshot.summary ?? null,
    lastSeq: snapshot.last_seq,
  };
}

function textOf(value: unknown): string {
  if (value && typeof value === "object" && "text" in value) {
    const text = (value as { text?: unknown }).text;
    return typeof text === "string" ? text : "";
  }
  return typeof value === "string" ? value : "";
}

function messageFromPayload(
  payload: Record<string, unknown>,
  fallbackSeq: number,
): RoomMessageDto {
  const mentions = Array.isArray(payload.mentions)
    ? payload.mentions.map(String)
    : [];
  const seq = Number(payload.seq);
  return {
    id: String(payload.message_id ?? ""),
    conversation_id: String(payload.conversation_id ?? ""),
    seq: Number.isFinite(seq) && seq > 0 ? seq : fallbackSeq,
    role: (payload.role as RoomMessageDto["role"]) ?? "system",
    sender: typeof payload.sender === "string" ? payload.sender : null,
    text: typeof payload.text === "string" ? payload.text : textOf(payload.text),
    mentions,
    quote_id: typeof payload.quote_id === "string" ? payload.quote_id : null,
    task_id: typeof payload.task_id === "string" ? payload.task_id : null,
    node_id: typeof payload.node_id === "string" ? payload.node_id : null,
    intervention_id:
      typeof payload.intervention_id === "string"
        ? payload.intervention_id
        : null,
    queued_for_node_id:
      typeof payload.queued_for_node_id === "string"
        ? payload.queued_for_node_id
        : null,
    delivered_at: null,
    created_at:
      typeof payload.created_at === "string"
        ? payload.created_at
        : new Date().toISOString(),
  };
}

export function applyRoomEvent(view: RoomView, event: EventDto): RoomView {
  const payload = event.payload;
  switch (event.type) {
    case "message.posted": {
      const message = messageFromPayload(payload, event.seq);
      const lastSeq = Math.max(view.lastSeq, message.seq);
      if (view.messages.some((item) => item.id === message.id)) {
        return { ...view, lastSeq };
      }
      return {
        ...view,
        messages: [...view.messages, message].sort(
          (left, right) => left.seq - right.seq,
        ),
        lastSeq,
      };
    }
    case "message.delivered": {
      const id = String(payload.message_id ?? "");
      return {
        ...view,
        messages: view.messages.map((item) =>
          item.id === id
            ? { ...item, delivered_at: new Date().toISOString() }
            : item,
        ),
      };
    }
    case "room.participant_joined": {
      const agentName = String(payload.agent_name ?? "");
      if (!agentName || view.members.some((m) => m.agent_name === agentName)) {
        return view;
      }
      return {
        ...view,
        members: [
          ...view.members,
          {
            conversation_id: String(payload.conversation_id ?? ""),
            agent_name: agentName,
            agent_url: String(payload.agent_url ?? ""),
            reason:
              typeof payload.reason === "string" ? payload.reason : null,
            joined_at:
              typeof payload.joined_at === "string"
                ? payload.joined_at
                : new Date().toISOString(),
          },
        ],
      };
    }
    case "room.summary_updated": {
      const summary =
        payload.summary && typeof payload.summary === "object"
          ? (payload.summary as Record<string, unknown>)
          : {};
      return {
        ...view,
        summary: {
          conversation_id: String(payload.conversation_id ?? ""),
          covers_seq: Number(payload.covers_seq ?? 0),
          summary,
          updated_at: new Date().toISOString(),
        },
      };
    }
    default:
      return view;
  }
}
