import { TaskState, taskStateToJSON } from "@a2a-js/sdk";
import { describe, expect, it } from "vitest";

import {
  applyStreamEvent,
  emptyConversation,
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
  let view = emptyConversation;
  view = applyStreamEvent(
    view,
    statusUpdate("state_delta", {
      nodes: {
        n1: { name: "n1", agent_name: "echo", status: "pending" },
        n2: { name: "n2", agent_name: "writer", status: "dispatched" },
      },
    }),
    0,
  );
  return view;
}

function functionCallEvent(
  name: string,
  args: Record<string, unknown> = {},
  result: Record<string, unknown> = { success: true },
) {
  return {
    payload: {
      $case: "artifactUpdate",
      value: {
        artifact: {
          artifactId: `fc-${name}`,
          parts: [{
            content: {
              $case: "data",
              value: {
                function_name: name,
                function_args: args,
                function_result: result,
              },
            },
            metadata: { cw_type: "function_call" },
          }],
        },
        append: false,
        lastChunk: true,
      },
    },
  };
}

describe("applyStreamEvent", () => {
  it("renders create_plan function call as a plan", () => {
    const view = applyStreamEvent(
      emptyConversation,
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
      emptyConversation,
      functionCallEvent(
        "create_plan",
        {},
        { success: false, error: "no agents registered" },
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

  it("applies state_delta: invalidates and cancels nodes", () => {
    let view = applyStreamEvent(
      viewWithNodes(),
      statusUpdate("state_delta", { nodes: { n1: { status: "invalidated" } } }),
      1,
    );
    expect(view.nodes[0].status).toBe("invalidated");
    view = applyStreamEvent(
      view,
      statusUpdate("state_delta", { nodes: { n2: { status: "canceled" } } }),
      2,
    );
    expect(view.nodes[1].status).toBe("canceled");
  });

  it("applies state_delta: marks node input_required", () => {
    const view = applyStreamEvent(
      viewWithNodes(),
      statusUpdate(
        "state_delta",
        { nodes: { n1: { status: "input_required", question: "预算口径？" } } },
        TaskState.TASK_STATE_INPUT_REQUIRED,
      ),
      1,
    );
    expect(view.nodes[0].status).toBe("input_required");
    expect(view.state).toBe(taskStateToJSON(TaskState.TASK_STATE_INPUT_REQUIRED));
  });

  it("applies state_delta: confirm_cancel intervention notification", () => {
    const view = applyStreamEvent(
      viewWithNodes(),
      statusUpdate("state_delta", {
        interventions: {
          iv1: {
            status: "pending",
            node_id: "n2",
            kind: "confirm_cancel",
            question: "是否打断？",
          },
        },
      }),
      1,
    );
    expect(view.nodes[1].status).toBe("dispatched");
    expect(view.state).toBe(taskStateToJSON(TaskState.TASK_STATE_WORKING));
    expect(view.notifications.at(-1)?.text).toContain("待确认");
  });

  it("applies state_delta: resolved and expired intervention notifications", () => {
    let view = applyStreamEvent(
      viewWithNodes(),
      statusUpdate("state_delta", {
        interventions: {
          iv1: { status: "pending", node_id: "n1", kind: "question" },
        },
      }),
      1,
    );
    view = applyStreamEvent(
      view,
      statusUpdate("state_delta", {
        interventions: {
          iv1: { status: "resolved", node_id: "n1", kind: "question" },
        },
      }),
      2,
    );
    expect(view.notifications.at(-1)?.text).toContain("任务继续");
    view = applyStreamEvent(
      view,
      statusUpdate("state_delta", {
        interventions: {
          iv2: { status: "expired", node_id: "n1", kind: "confirm_cancel" },
        },
      }),
      3,
    );
    expect(view.notifications.at(-1)?.text).toContain("无需处理");
  });

  it("applies state_delta: adds new members with notification", () => {
    const view = applyStreamEvent(
      emptyConversation,
      statusUpdate("state_delta", {
        members: [
          { agent_name: "writer", agent_url: "http://x", reason: "plan" },
        ],
      }),
      1,
    );
    expect(view.members.map((m) => m.agent_name)).toContain("writer");
    expect(view.notifications.at(-1)?.text).toContain("writer");
    expect(view.notifications.at(-1)?.text).toContain("加入了群聊");
  });

  it("replay: state_delta restores members, nodes and interventions", () => {
    const view = applyStreamEvent(
      emptyConversation,
      statusUpdate("state_delta", {
        nodes: {
          n1: { name: "task1", agent_name: "echo", status: "completed", output: "done" },
        },
        members: [
          { agent_name: "echo", agent_url: "http://echo", reason: "plan" },
          { agent_name: "writer", agent_url: "http://writer", reason: "plan" },
        ],
        interventions: {
          iv1: { status: "pending", node_id: "n1", kind: "confirm_cancel", question: "预算口径？" },
        },
      }),
      0,
    );
    expect(view.members.map((m) => m.agent_name)).toEqual(["echo", "writer"]);
    expect(view.nodes.map((n) => n.id)).toEqual(["n1"]);
    expect(view.nodes[0].status).toBe("completed");
    expect(view.nodes[0].output).toBe("done");
    expect(Object.keys(view.interventions)).toEqual(["iv1"]);
    expect(view.interventions.iv1.status).toBe("pending");
    expect(view.interventions.iv1.kind).toBe("confirm_cancel");
    expect(view.notifications.some((n) => n.text.includes("加入了群聊"))).toBe(true);
    expect(view.notifications.some((n) => n.kind === "intervention.requested")).toBe(true);
  });

  it("streaming artifact: append chunk updates same bubble, lastChunk removes it", () => {
    let view = viewWithNodes();
    const artifactId = "art-stream-1";
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId, parts: [{ content: { $case: "text", value: "Hello" } }] },
            metadata: { node_id: "n1", agent_name: "echo" },
            append: false,
            lastChunk: false,
          },
        },
      },
      1,
    );
    expect(view.workingBubbles).toHaveLength(1);
    expect(view.workingBubbles[0].artifactId).toBe(artifactId);
    expect(view.workingBubbles[0].text).toBe("Hello");
    expect(view.activeArtifactIds.has(artifactId)).toBe(true);

    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId, parts: [{ content: { $case: "text", value: " world" } }] },
            metadata: { node_id: "n1", agent_name: "echo" },
            append: true,
            lastChunk: false,
          },
        },
      },
      2,
    );
    expect(view.workingBubbles).toHaveLength(1);
    expect(view.workingBubbles[0].text).toBe("Hello world");

    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId, parts: [{ content: { $case: "text", value: "!" } }] },
            metadata: { node_id: "n1", agent_name: "echo" },
            append: true,
            lastChunk: true,
          },
        },
      },
      3,
    );
    expect(view.workingBubbles).toHaveLength(0);
    expect(view.activeArtifactIds.has(artifactId)).toBe(false);
    expect(view.nodes.find((n) => n.id === "n1")?.output).toBe("Hello world!");
  });

  it("two concurrent streams without node_id create distinct bubbles", () => {
    let view = emptyConversation;
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId: "art-a", parts: [{ content: { $case: "text", value: "A1" } }] },
            append: false,
            lastChunk: false,
          },
        },
      },
      1,
    );
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId: "art-b", parts: [{ content: { $case: "text", value: "B1" } }] },
            append: false,
            lastChunk: false,
          },
        },
      },
      2,
    );
    expect(view.workingBubbles).toHaveLength(2);
    expect(view.workingBubbles.find((b) => b.artifactId === "art-a")?.text).toBe("A1");
    expect(view.workingBubbles.find((b) => b.artifactId === "art-b")?.text).toBe("B1");
  });
});
