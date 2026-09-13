import { describe, expect, it } from "vitest";

import { applyRoomEvent, fromRoomSnapshot, mergeRoomSnapshot } from "./roomView";
import type { EventDto, RoomMessageDto, RoomMessagesDto } from "./types";

function message(overrides: Partial<RoomMessageDto> = {}): RoomMessageDto {
  return {
    id: overrides.id ?? `m${overrides.seq ?? 1}`,
    conversation_id: "c1",
    seq: overrides.seq ?? 1,
    role: overrides.role ?? "user",
    sender: overrides.sender ?? "CEO",
    text: overrides.text ?? "你好",
    mentions: overrides.mentions ?? [],
    created_at: "2026-09-13T00:00:00+00:00",
    ...overrides,
  };
}

function snapshot(messages: RoomMessageDto[] = []): RoomMessagesDto {
  return {
    messages,
    members: [],
    summary: null,
    last_seq: messages.reduce((max, item) => Math.max(max, item.seq), 0),
  };
}

function event(
  type: string,
  seq: number,
  payload: Record<string, unknown>,
): EventDto {
  return { seq, type, payload };
}

describe("roomView", () => {
  it("loads a sorted snapshot", () => {
    const view = fromRoomSnapshot(snapshot([message({ seq: 2, id: "b" }), message({ seq: 1, id: "a" })]));
    expect(view.messages.map((item) => item.id)).toEqual(["a", "b"]);
    expect(view.lastSeq).toBe(2);
  });

  it("appends and dedupes posted messages", () => {
    let view = fromRoomSnapshot(snapshot());
    view = applyRoomEvent(
      view,
      event("message.posted", 1, {
        message_id: "m1",
        seq: 1,
        role: "user",
        sender: "CEO",
        text: "hi",
      }),
    );
    view = applyRoomEvent(
      view,
      event("message.posted", 1, {
        message_id: "m1",
        seq: 1,
        role: "user",
        sender: "CEO",
        text: "hi",
      }),
    );
    view = applyRoomEvent(
      view,
      event("message.posted", 2, {
        message_id: "m2",
        seq: 2,
        role: "agent",
        sender: "researcher",
        text: "done",
      }),
    );
    expect(view.messages.map((item) => item.id)).toEqual(["m1", "m2"]);
    expect(view.lastSeq).toBe(2);
  });

  it("marks queued messages delivered", () => {
    let view = fromRoomSnapshot(
      snapshot([message({ id: "m1", queued_for_node_id: "p1:n1" })]),
    );
    view = applyRoomEvent(
      view,
      event("message.delivered", 2, { message_id: "m1", node_id: "p1:n1" }),
    );
    expect(view.messages[0].delivered_at).toBeTruthy();
  });

  it("adds members and replaces the summary", () => {
    let view = fromRoomSnapshot(snapshot());
    view = applyRoomEvent(
      view,
      event("room.participant_joined", 3, {
        agent_name: "researcher",
        agent_url: "http://r",
        reason: "human_mention",
      }),
    );
    view = applyRoomEvent(
      view,
      event("room.participant_joined", 4, {
        agent_name: "researcher",
        agent_url: "http://r",
      }),
    );
    expect(view.members.map((member) => member.agent_name)).toEqual([
      "researcher",
    ]);

    view = applyRoomEvent(
      view,
      event("room.summary_updated", 5, {
        covers_seq: 4,
        summary: { goal: "调研" },
      }),
    );
    expect(view.summary?.covers_seq).toBe(4);
    expect(view.summary?.summary.goal).toBe("调研");
  });

  it("ignores unrelated events", () => {
    const view = fromRoomSnapshot(snapshot([message({ id: "m1" })]));
    const next = applyRoomEvent(view, event("node.artifact", 9, { text: "x" }));
    expect(next).toBe(view);
  });
});

describe("mergeRoomSnapshot", () => {
  it("keeps live messages and merges the rest", () => {
    const live = message({ id: "m2", seq: 2, text: "实时" });
    const view = { ...fromRoomSnapshot(snapshot([live])), lastSeq: 2 };
    const merged = mergeRoomSnapshot(
      view,
      snapshot([message({ id: "m1", seq: 1 }), live]),
    );
    expect(merged.messages.map((item) => item.id)).toEqual(["m1", "m2"]);
    expect(merged.lastSeq).toBe(2);
  });

  it("keeps the newer summary and unions members", () => {
    const view = fromRoomSnapshot({
      messages: [],
      members: [{ conversation_id: "c1", agent_name: "a", agent_url: "u", joined_at: "" }],
      summary: { conversation_id: "c1", covers_seq: 5, summary: {}, updated_at: "" },
      last_seq: 0,
    });
    const merged = mergeRoomSnapshot(view, {
      messages: [],
      members: [{ conversation_id: "c1", agent_name: "b", agent_url: "u", joined_at: "" }],
      summary: { conversation_id: "c1", covers_seq: 3, summary: { old: 1 }, updated_at: "" },
      last_seq: 0,
    });
    expect(merged.members.map((member) => member.agent_name)).toEqual(["a", "b"]);
    expect(merged.summary?.covers_seq).toBe(5);
  });
});
