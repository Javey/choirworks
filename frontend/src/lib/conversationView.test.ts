import { TaskState, taskStateToJSON } from "@a2a-js/sdk";
import { describe, expect, it } from "vitest";

import {
  applyStreamEvent,
  conversationFromTasks,
  type ConversationView,
} from "./conversationView";

function statusUpdate(
  kind: string,
  meta: Record<string, unknown> = {},
  state: TaskState = TaskState.TASK_STATE_WORKING,
) {
  return {
    payload: {
      $case: "statusUpdate",
      value: {
        status: { state },
        metadata: { ...meta, kind },
      },
    },
  };
}

function viewWithNodes(): ConversationView {
  return conversationFromTasks([], "c1", {
    nodes: [
      { id: "n1", name: "n1", agent_name: "echo", status: "pending" },
      { id: "n2", name: "n2", agent_name: "writer", status: "dispatched" },
    ],
  });
}

describe("applyStreamEvent", () => {
  it("applies plan revisions: adds and invalidates nodes", () => {
    const view = applyStreamEvent(
      viewWithNodes(),
      statusUpdate("plan.revised", {
        reason: "换人",
        added_nodes: [
          { id: "x1", name: "designer", agent_name: "designer", deps: ["n1"] },
        ],
        invalidated: ["n2"],
      }),
      1,
    );
    expect(view.nodes.map((n) => [n.id, n.status])).toEqual([
      ["n1", "pending"],
      ["n2", "invalidated"],
      ["x1", "pending"],
    ]);
    expect(view.nodes[2].agent_name).toBe("designer");
    expect(view.notifications.at(-1)?.text).toContain("计划已修订");
  });

  it("marks invalidated and canceled nodes", () => {
    let view = applyStreamEvent(
      viewWithNodes(),
      statusUpdate("node.invalidated", { node_id: "n1" }),
      1,
    );
    expect(view.nodes[0].status).toBe("invalidated");
    view = applyStreamEvent(
      view,
      statusUpdate("node.canceled", { node_id: "n2" }),
      2,
    );
    expect(view.nodes[1].status).toBe("canceled");
  });

  it("shows intervention questions and marks the node waiting", () => {
    const view = applyStreamEvent(
      viewWithNodes(),
      statusUpdate("intervention.requested", {
        node_id: "n1",
        intervention_id: "iv1",
        question: "预算口径？",
      }),
      1,
    );
    expect(view.nodes[0].status).toBe("input_required");
    expect(view.state).toBe(taskStateToJSON(TaskState.TASK_STATE_INPUT_REQUIRED));
    expect(view.notifications.at(-1)?.text).toBe("预算口径？");
  });

  it("shows confirm_cancel requests without pausing the task", () => {
    const view = applyStreamEvent(
      viewWithNodes(),
      statusUpdate("intervention.requested", {
        node_id: "n2",
        intervention_id: "iv1",
        intervention_kind: "confirm_cancel",
        question: "是否打断？",
      }),
      1,
    );
    expect(view.nodes[1].status).toBe("dispatched");
    expect(view.state).toBe(taskStateToJSON(TaskState.TASK_STATE_WORKING));
    expect(view.notifications.at(-1)?.text).toContain("待确认");
  });

  it("notifies resolved and expired interventions", () => {
    let view = applyStreamEvent(
      viewWithNodes(),
      statusUpdate("intervention.resolved", { intervention_id: "iv1" }),
      1,
    );
    expect(view.notifications.at(-1)?.text).toContain("任务继续");
    view = applyStreamEvent(
      view,
      statusUpdate("intervention.expired", { intervention_id: "iv2" }),
      2,
    );
    expect(view.notifications.at(-1)?.text).toContain("无需处理");
  });
});
