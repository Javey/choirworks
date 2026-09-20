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

function functionCallEvent(
  name: string,
  args: Record<string, unknown> = {},
  result: Record<string, unknown> = { success: true },
  state: TaskState = TaskState.TASK_STATE_WORKING,
) {
  return {
    payload: {
      $case: "statusUpdate",
      value: {
        status: { state },
        metadata: {
          kind: "function_call",
          function_name: name,
          function_args: args,
          function_result: result,
        },
      },
    },
  };
}

describe("applyStreamEvent", () => {
  it("renders create_plan function call as a plan", () => {
    const view = applyStreamEvent(
      conversationFromTasks([], "c1"),
      functionCallEvent("create_plan", {
        nodes: [
          { id: "n1", name: "task1", agent_name: "echo", deps: [] },
          { id: "n2", name: "task2", agent_name: "writer", deps: ["n1"] },
        ],
      }),
      1,
    );
    expect(view.nodes.map((n) => [n.id, n.status])).toEqual([
      ["n1", "pending"],
      ["n2", "pending"],
    ]);
    expect(view.notifications.at(-1)?.text).toBe("执行计划：2 个节点");
  });

  it("renders create_plan function call failure", () => {
    const view = applyStreamEvent(
      conversationFromTasks([], "c1"),
      functionCallEvent(
        "create_plan",
        {},
        { success: false, error: "no agents registered" },
        TaskState.TASK_STATE_FAILED,
      ),
      1,
    );
    expect(view.notifications.at(-1)?.text).toContain("计划失败");
    expect(view.notifications.at(-1)?.text).toContain("no agents registered");
  });

  it("renders revise_plan function call: adds and invalidates nodes", () => {
    const view = applyStreamEvent(
      viewWithNodes(),
      functionCallEvent(
        "revise_plan",
        { reason: "换人", patch: { add: [], invalidate: [] } },
        {
          success: true,
          reason: "换人",
          added_nodes: [
            { id: "x1", name: "designer", agent_name: "designer", deps: ["n1"] },
          ],
          invalidated: ["n2"],
        },
      ),
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

  it("renders ask_user function call: marks node waiting and shows question", () => {
    const view = applyStreamEvent(
      viewWithNodes(),
      functionCallEvent(
        "ask_user",
        { node_id: "n1", question: "预算口径？" },
        {
          success: true,
          intervention_id: "iv1",
          node_id: "n1",
          question: "预算口径？",
        },
        TaskState.TASK_STATE_INPUT_REQUIRED,
      ),
      1,
    );
    expect(view.nodes[0].status).toBe("input_required");
    expect(view.state).toBe(taskStateToJSON(TaskState.TASK_STATE_INPUT_REQUIRED));
    expect(view.notifications.at(-1)?.text).toBe("预算口径？");
  });

  it("renders call_subagent function call: adds helper node", () => {
    const view = applyStreamEvent(
      viewWithNodes(),
      functionCallEvent(
        "call_subagent",
        { requester_node_id: "n1", target_agent: "writer", instruction: "帮忙写" },
        {
          success: true,
          helper_node_id: "n1-h1",
          helper: "writer",
          requester: "echo",
        },
      ),
      1,
    );
    expect(view.nodes.map((n) => n.id)).toContain("n1-h1");
    expect(view.notifications.at(-1)?.text).toContain("echo");
    expect(view.notifications.at(-1)?.text).toContain("writer");
  });

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
