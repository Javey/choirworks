import { describe, expect, it } from "vitest";

import { buildTimeline } from "./timeline";
import type { TaskView } from "./taskView";

function view(overrides: Partial<TaskView>): TaskView {
  return {
    id: "t1",
    request: "你好",
    status: "running",
    conversationId: "c1",
    nodes: [],
    nodeOrder: [],
    interventions: [],
    notes: [],
    connection: "live",
    lastSeq: 1,
    createdAt: "2026-09-13T00:00:00+00:00",
    updatedAt: "2026-09-13T00:00:00+00:00",
    ...overrides,
  };
}

describe("buildTimeline", () => {
  it("orders user, plan, nodes, interventions, notes and result", () => {
    const completed = view({
      id: "t1",
      status: "completed",
      nodeOrder: ["p1:n1", "p1:n2"],
      nodes: [
        {
          id: "p1:n1",
          name: "n1",
          agentName: "echo",
          status: "completed",
          attempt: 1,
          inputText: "hi",
          outputText: "echo:hi",
          order: 0,
        },
        {
          id: "p1:n2",
          name: "n2",
          agentName: "echo",
          status: "completed",
          attempt: 1,
          outputText: "done",
          order: 1,
        },
      ],
      interventions: [
        {
          id: "iv1",
          status: "resolved",
          policy: "human",
          source: "remote_input_required",
          questionText: "q?",
          answerText: "A",
        },
      ],
      notes: [{ id: "n", level: "info", text: "note" }],
    });

    const items = buildTimeline([completed]);
    expect(items.map((item) => item.kind)).toEqual([
      "user",
      "plan",
      "node",
      "node",
      "intervention",
      "note",
      "result",
    ]);
    const result = items.at(-1);
    expect(result).toMatchObject({ kind: "result", status: "completed", text: "done" });
  });

  it("produces a failed result with the first node error", () => {
    const failed = view({
      status: "failed",
      nodeOrder: ["p1:n1"],
      nodes: [
        {
          id: "p1:n1",
          name: "n1",
          status: "failed",
          attempt: 2,
          error: "boom",
          order: 0,
        },
      ],
    });
    const items = buildTimeline([failed]);
    const result = items.at(-1);
    expect(result).toMatchObject({ kind: "result", status: "failed", error: "boom" });
  });

  it("keeps chronological task order across a conversation", () => {
    const first = view({ id: "t1", status: "completed", request: "第一问" });
    const second = view({ id: "t2", status: "canceled", request: "第二问" });
    const items = buildTimeline([first, second]);
    expect(items.filter((item) => item.kind === "user").map((item) => item.taskId)).toEqual([
      "t1",
      "t2",
    ]);
  });
});
