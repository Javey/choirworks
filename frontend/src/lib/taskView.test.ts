import { describe, expect, it } from "vitest";

import { applyEvent, fromSnapshot } from "./taskView";
import type { EventDto, TaskSnapshotDto } from "./types";

function snapshot(): TaskSnapshotDto {
  return {
    task: {
      id: "t1",
      status: "running",
      request: "你好",
      conversation_id: "c1",
      created_at: "2026-09-13T00:00:00+00:00",
      updated_at: "2026-09-13T00:00:01+00:00",
    },
    plan: {
      id: "p1",
      task_id: "t1",
      version: 1,
      rationale: "first",
      created_at: "2026-09-13T00:00:00+00:00",
      dag: {
        nodes: [
          { id: "n1", name: "n1", agent_name: "echo", deps: [], input: { text: "hi" } },
        ],
      },
    },
    nodes: [
      {
        id: "p1:n1",
        task_id: "t1",
        plan_id: "p1",
        name: "n1",
        agent_name: "echo",
        status: "working",
        attempt: 1,
        input: { text: "hi" },
        output: null,
        error: null,
      },
    ],
    last_seq: 5,
  };
}

describe("fromSnapshot", () => {
  it("maps task, nodes and order", () => {
    const view = fromSnapshot(snapshot());
    expect(view.id).toBe("t1");
    expect(view.status).toBe("running");
    expect(view.conversationId).toBe("c1");
    expect(view.lastSeq).toBe(5);
    expect(view.nodeOrder).toEqual(["p1:n1"]);
    expect(view.nodes[0].inputText).toBe("hi");
    expect(view.nodes[0].status).toBe("working");
  });
});

describe("applyEvent", () => {
  it("ignores events at or before lastSeq", () => {
    const view = fromSnapshot(snapshot());
    const stale = applyEvent(view, {
      seq: 5,
      type: "task.completed",
      payload: {},
    });
    expect(stale).toBe(view);
  });

  it("folds node output, state and task terminal events", () => {
    let view = fromSnapshot(snapshot());
    view = applyEvent(view, {
      seq: 6,
      type: "node.output",
      payload: { node_id: "p1:n1", output: { artifacts: [{ text: "echo:hi" }] } },
    });
    expect(view.nodes[0].outputText).toBe("echo:hi");

    view = applyEvent(view, {
      seq: 7,
      type: "node.state_changed",
      payload: { node_id: "p1:n1", to: "completed" },
    });
    expect(view.nodes[0].status).toBe("completed");

    view = applyEvent(view, { seq: 8, type: "task.completed", payload: {} });
    expect(view.status).toBe("completed");
    expect(view.lastSeq).toBe(8);
  });

  it("adds nodes from a superseding plan and marks old ones", () => {
    let view = fromSnapshot(snapshot());
    view = applyEvent(view, {
      seq: 6,
      type: "plan.created",
      payload: {
        plan_id: "p2",
        version: 2,
        rationale: "replan",
        dag: {
          nodes: [
            { id: "n2", name: "n2", agent_name: "echo", deps: [], input: { text: "again" } },
          ],
        },
      },
    });
    expect(view.nodeOrder).toEqual(["p1:n1", "p2:n2"]);
    expect(view.nodes.find((node) => node.id === "p1:n1")?.superseded).toBe(true);
    expect(view.nodes.find((node) => node.id === "p2:n2")?.status).toBe("pending");
  });

  it("tracks interventions", () => {
    let view = fromSnapshot(snapshot());
    const requested: EventDto = {
      seq: 6,
      type: "intervention.requested",
      payload: {
        intervention_id: "iv1",
        node_id: "p1:n1",
        source: "remote_input_required",
        policy: "human",
        question: { text: "which one?" },
      },
    };
    view = applyEvent(view, requested);
    expect(view.interventions).toHaveLength(1);
    expect(view.interventions[0].questionText).toBe("which one?");
    expect(view.interventions[0].status).toBe("pending");

    view = applyEvent(view, {
      seq: 7,
      type: "intervention.resolved",
      payload: { intervention_id: "iv1", answer: { text: "A" }, responder: "user" },
    });
    expect(view.interventions[0].status).toBe("resolved");
    expect(view.interventions[0].answerText).toBe("A");
  });

  it("records retry notes and rollback resets", () => {
    let view = fromSnapshot(snapshot());
    view = applyEvent(view, {
      seq: 6,
      type: "node.retry.scheduled",
      payload: { node_id: "p1:n1", attempt: 2, delay_seconds: 1 },
    });
    expect(view.notes.some((note) => note.text.includes("重试"))).toBe(true);
    expect(view.nodes[0].attempt).toBe(2);

    view = applyEvent(view, {
      seq: 7,
      type: "rollback.performed",
      payload: { checkpoint_id: "ck1", reset_node_ids: ["p1:n1"] },
    });
    expect(view.nodes[0].status).toBe("pending");
    expect(view.nodes[0].outputText).toBeUndefined();
    expect(view.notes.some((note) => note.text.includes("回退"))).toBe(true);
  });

  it("records errors on the node and as a note", () => {
    let view = fromSnapshot(snapshot());
    view = applyEvent(view, {
      seq: 6,
      type: "error",
      payload: { node_id: "p1:n1", message: "boom" },
    });
    expect(view.nodes[0].error).toBe("boom");
    expect(view.notes.some((note) => note.text === "boom")).toBe(true);
  });
});

import { mergeSnapshot } from "./taskView";

describe("mergeSnapshot", () => {
  it("takes server nodes but preserves local interventions and notes", () => {
    let view = fromSnapshot(snapshot());
    view = applyEvent(view, {
      seq: 6,
      type: "intervention.requested",
      payload: {
        intervention_id: "iv1",
        node_id: "p1:n1",
        source: "remote_input_required",
        policy: "human",
        question: { text: "q?" },
      },
    });
    view = applyEvent(view, {
      seq: 7,
      type: "node.output",
      payload: { node_id: "p1:n1", output: { artifacts: [{ text: "echo:hi" }] } },
    });

    const fresh = snapshot();
    fresh.task = { ...fresh.task, status: "completed" };
    fresh.nodes[0] = { ...fresh.nodes[0], status: "completed", output: { artifacts: [{ text: "echo:hi" }] } };
    fresh.last_seq = 8;

    const merged = mergeSnapshot(view, fresh);
    expect(merged.status).toBe("completed");
    expect(merged.nodes[0].status).toBe("completed");
    expect(merged.interventions).toHaveLength(1);
    expect(merged.notes.length).toBe(view.notes.length);
    expect(merged.lastSeq).toBeGreaterThanOrEqual(8);
  });
});
