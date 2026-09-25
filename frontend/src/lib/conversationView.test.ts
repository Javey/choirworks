import { TaskState, taskStateToJSON } from "@a2a-js/sdk";
import { describe, expect, it } from "vitest";

import {
  applyStreamEvent,
  emptyConversation,
  mergeConsecutiveJoins,
  type ConversationView,
  type TimelineItem,
} from "./conversationView";

type ProtoStruct = Record<string, unknown>;

function statusUpdate(
  kind: string,
  meta: Record<string, unknown> = {},
  state: TaskState = TaskState.TASK_STATE_WORKING,
) {
  const metadata = kind === "state_delta" ? { cw_delta: meta } : meta;
  return {
    payload: {
      $case: "statusUpdate",
      value: {
        status: { state },
        metadata,
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
        n1: { name: "n1", agent_name: "echo", status: "pending", input_text: "任务一" },
        n2: { name: "n2", agent_name: "writer", status: "dispatched", input_text: "任务二" },
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
          { id: "n1", name: "task1", agent_name: "echo", deps: [], input: { text: "任务一" } },
          { id: "n2", name: "task2", agent_name: "writer", deps: ["n1"], input: { text: "任务二" } },
        ],
      }),
      1,
    );
    expect(view.nodes.map((n) => [n.id, n.status])).toEqual([
      ["n1", "pending"],
      ["n2", "pending"],
    ]);
    expect(view.nodes[0].input_text).toBe("任务一");
    expect(view.nodes[1].input_text).toBe("任务二");
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
          data: {
            reason: "换人",
            added_nodes: [
              { id: "x1", name: "designer", agent_name: "designer", deps: ["n1"], input_text: "新任务" },
            ],
            invalidated: ["n2"],
          },
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
    expect(view.nodes[2].input_text).toBe("新任务");
    expect(view.notifications.at(-1)?.text).toContain("计划已修订");
  });

  it("marks a node input_required from a state delta", () => {
    const view = applyStreamEvent(
      viewWithNodes(),
      statusUpdate("state_delta", {
        nodes: { n1: { status: "input_required", question: "预算口径？" } },
      }),
      1,
    );
    expect(view.nodes[0].status).toBe("input_required");
  });

  it("renders call_subagent assist as dispatch bubble, not agent message", () => {
    const view = applyStreamEvent(
      viewWithNodes(),
      functionCallEvent(
        "call_subagent",
        { requested_by: "n1", target_agent: "writer", instruction: "帮忙写" },
        {
          success: true,
          data: { helper_node_id: "n1-h1", helper: "writer", requester: "echo" },
        },
      ),
      1,
    );
    expect(view.nodes.map((n) => n.id)).toContain("n1-h1");
    expect(view.nodes.find((n) => n.id === "n1-h1")?.input_text).toBe("帮忙写");
    expect(view.notifications.some((n) => n.kind === "assist.dispatched")).toBe(false);
    const msg = view.messages.at(-1);
    expect(msg?.role).toBe("assistant");
    expect(msg?.group).toBe("dispatch");
    expect(msg?.text).toBe("- @writer 帮忙写");
  });

  it("merges consecutive assistant dispatches into one bubble", () => {
    let view = viewWithNodes();
    view = applyStreamEvent(
      view,
      functionCallEvent(
        "call_subagent",
        { requested_by: "assistant", target_agent: "echo", instruction: "任务甲" },
        { success: true },
      ),
      1,
    );
    view = applyStreamEvent(
      view,
      functionCallEvent(
        "call_subagent",
        { requested_by: "assistant", target_agent: "writer", instruction: "任务乙" },
        { success: true },
      ),
      2,
    );
    expect(view.messages).toHaveLength(1);
    expect(view.messages[0].role).toBe("assistant");
    expect(view.messages[0].group).toBe("dispatch");
    expect(view.messages[0].text).toBe("- @echo 任务甲\n- @writer 任务乙");
    expect(view.notifications).toHaveLength(0);
  });

  it("starts a new dispatch bubble after an intervening message", () => {
    let view = viewWithNodes();
    view = applyStreamEvent(
      view,
      functionCallEvent(
        "call_subagent",
        { requested_by: "assistant", target_agent: "echo", instruction: "任务甲" },
        { success: true },
      ),
      1,
    );
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: {
              artifactId: "text-1",
              parts: [{ content: { $case: "text", value: "下一步" } }],
              metadata: { author: "assistant" },
            },
            append: false,
            lastChunk: true,
          },
        },
      },
      2,
    );
    view = applyStreamEvent(
      view,
      functionCallEvent(
        "call_subagent",
        { requested_by: "assistant", target_agent: "writer", instruction: "任务丙" },
        { success: true },
      ),
      3,
    );
    expect(view.messages).toHaveLength(3);
    expect(view.messages[0].text).toBe("- @echo 任务甲");
    expect(view.messages[2].text).toBe("- @writer 任务丙");
    expect(view.messages[2].group).toBe("dispatch");
  });

  it("does not duplicate a replayed dispatch line", () => {
    let view = viewWithNodes();
    const event = functionCallEvent(
      "call_subagent",
      { requested_by: "assistant", target_agent: "echo", instruction: "任务甲" },
      { success: true },
    );
    view = applyStreamEvent(view, event, 1);
    view = applyStreamEvent(view, event, 2);
    expect(view.messages).toHaveLength(1);
    expect(view.messages[0].text).toBe("- @echo 任务甲");
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

  it("resolved intervention emits a human-answer notification", () => {
    const pending = statusUpdate("state_delta", {
      interventions: {
        iv1: { status: "pending", node_id: "n1", kind: "question" },
      },
    });
    let view = applyStreamEvent(viewWithNodes(), pending, 1);
    view = applyStreamEvent(
      view,
      statusUpdate("state_delta", {
        interventions: {
          iv1: {
            status: "resolved",
            node_id: "n1",
            kind: "question",
            responder: "human",
          },
        },
      }),
      2,
    );
    expect(view.interventions.iv1.status).toBe("resolved");
    expect(
      view.notifications.some((n) => n.text === "人工答复已回填，任务继续"),
    ).toBe(true);
  });

  it("applies state_delta: adds new members without notification", () => {
    const view = applyStreamEvent(
      emptyConversation,
      statusUpdate("state_delta", {
        members: [
          {
            agent_name: "writer",
            agent_url: "http://x",
            reason: "plan",
            joined_at: "2026-01-01T00:00:00Z",
          },
        ],
      }),
      1,
    );
    expect(view.members.map((m) => m.agent_name)).toContain("writer");
    expect(view.members[0].joined_at).toBe("2026-01-01T00:00:00Z");
    expect(view.notifications.some((n) => n.text.includes("加入了群聊"))).toBe(false);
  });

  it("renders join_members function call as join notifications", () => {
    const view = applyStreamEvent(
      emptyConversation,
      functionCallEvent(
        "join_members",
        { names: ["writer", "reviewer"], reason: "human_mention" },
        { success: true, data: { joined: ["writer", "reviewer"] } },
      ),
      1,
    );
    const joins = view.notifications.filter(
      (n) => n.kind === "room.participant_joined",
    );
    expect(joins.map((n) => n.text)).toEqual([
      "writer 加入了群聊",
      "reviewer 加入了群聊",
    ]);
  });

  it("replay: state_delta restores members, nodes and interventions", () => {
    const view = applyStreamEvent(
      emptyConversation,
      statusUpdate("state_delta", {
        nodes: {
          n1: { name: "task1", agent_name: "echo", status: "completed", output: "done", input_text: "任务一" },
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
    expect(view.nodes[0].input_text).toBe("任务一");
    expect(Object.keys(view.interventions)).toEqual(["iv1"]);
    expect(view.interventions.iv1.status).toBe("pending");
    expect(view.interventions.iv1.kind).toBe("confirm_cancel");
    expect(view.notifications.some((n) => n.text.includes("加入了群聊"))).toBe(false);
    expect(view.notifications.some((n) => n.kind === "intervention.requested")).toBe(true);
  });

  it("streaming artifact: lastChunk materializes the bubble as an agent message", () => {
    let view = viewWithNodes();
    const artifactId = "art-stream-1";
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId, parts: [{ content: { $case: "text", value: "Hello" } }], metadata: { node_id: "n1", author: "echo" } },
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
            artifact: { artifactId, parts: [{ content: { $case: "text", value: " world" } }], metadata: { node_id: "n1", author: "echo" } },
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
            artifact: { artifactId, parts: [{ content: { $case: "text", value: "!" } }], metadata: { node_id: "n1", author: "echo" } },
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
    const msg = view.messages.find((m) => m.id === artifactId);
    expect(msg?.role).toBe("agent");
    expect(msg?.sender).toBe("echo");
    expect(msg?.node_id).toBe("n1");
    expect(msg?.text).toBe("Hello world!");
  });

  it("terminal state_delta materializes an unfinished stream as agent bubble", () => {
    let view = viewWithNodes();
    const artifactId = "art-unfinished";
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId, parts: [{ content: { $case: "text", value: "部分产出" } }], metadata: { node_id: "n1", author: "echo" } },
            append: false,
            lastChunk: false,
          },
        },
      },
      1,
    );
    expect(view.workingBubbles).toHaveLength(1);

    view = applyStreamEvent(
      view,
      statusUpdate("state_delta", {
        nodes: { n1: { status: "completed", agent_name: "echo", output: "部分产出" } },
      }),
      2,
    );

    expect(view.workingBubbles).toHaveLength(0);
    expect(view.nodes.find((n) => n.id === "n1")?.status).toBe("completed");
    const msg = view.messages.find((m) => m.id === artifactId);
    expect(msg?.role).toBe("agent");
    expect(msg?.sender).toBe("echo");
    expect(msg?.text).toBe("部分产出");
  });

  it("late lastChunk after completed keeps status and appends to the materialized message", () => {
    let view = viewWithNodes();
    const artifactId = "art-late";
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId, parts: [{ content: { $case: "text", value: "Hello" } }], metadata: { node_id: "n1", author: "echo" } },
            append: false,
            lastChunk: false,
          },
        },
      },
      1,
    );
    view = applyStreamEvent(
      view,
      statusUpdate("state_delta", {
        nodes: { n1: { status: "completed", agent_name: "echo", output: "Hello" } },
      }),
      2,
    );
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId, parts: [{ content: { $case: "text", value: " world!" } }], metadata: { node_id: "n1", author: "echo" } },
            append: true,
            lastChunk: true,
          },
        },
      },
      3,
    );

    expect(view.nodes.find((n) => n.id === "n1")?.status).toBe("completed");
    expect(view.workingBubbles).toHaveLength(0);
    expect(view.messages.find((m) => m.id === artifactId)?.text).toBe("Hello world!");
  });

  it("failed node keeps partially streamed text as agent bubble", () => {
    let view = viewWithNodes();
    const artifactId = "art-partial";
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId, parts: [{ content: { $case: "text", value: "写到一半" } }], metadata: { node_id: "n2", author: "writer" } },
            append: false,
            lastChunk: false,
          },
        },
      },
      1,
    );
    view = applyStreamEvent(
      view,
      statusUpdate("state_delta", {
        nodes: { n2: { status: "failed", agent_name: "writer", error: "boom" } },
      }),
      2,
    );

    expect(view.workingBubbles).toHaveLength(0);
    const msg = view.messages.find((m) => m.id === artifactId);
    expect(msg?.role).toBe("agent");
    expect(msg?.sender).toBe("writer");
    expect(msg?.node_id).toBe("n2");
    expect(msg?.text).toBe("写到一半");
  });

  it("two concurrent streams with node_id create distinct bubbles", () => {
    let view = emptyConversation;
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: { artifactId: "art-a", parts: [{ content: { $case: "text", value: "A1" } }], metadata: { node_id: "n1", author: "echo" } },
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
            artifact: { artifactId: "art-b", parts: [{ content: { $case: "text", value: "B1" } }], metadata: { node_id: "n2", author: "writer" } },
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

  it("streaming thought accumulates across chunks", () => {
    let view = emptyConversation;
    const id = "thought-1";
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: {
              artifactId: id,
              parts: [{
                content: { $case: "text", value: "The" },
                metadata: { cw_thought: true },
              }],
            },
            metadata: { author: "assistant" },
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
            artifact: {
              artifactId: id,
              parts: [{
                content: { $case: "text", value: " user" },
                metadata: { cw_thought: true },
              }],
            },
            metadata: { author: "assistant" },
            append: true,
            lastChunk: false,
          },
        },
      },
      2,
    );
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: {
              artifactId: id,
              parts: [{
                content: { $case: "text", value: " said hello" },
                metadata: { cw_thought: true },
              }],
            },
            metadata: { author: "assistant" },
            append: true,
            lastChunk: true,
          },
        },
      },
      3,
    );
    const msg = view.messages.find((m) => m.id === id);
    expect(msg?.text).toBe("The user said hello");
    expect(msg?.thinking).toBe(true);
    expect(view.activeArtifactIds.has(id)).toBe(false);
  });

  it("streaming assistant text response accumulates across chunks", () => {
    let view = emptyConversation;
    const id = "text-1";
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: {
              artifactId: id,
              parts: [{ content: { $case: "text", value: "你好" } }],
              metadata: { author: "assistant" },
            },
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
            artifact: {
              artifactId: id,
              parts: [{ content: { $case: "text", value: "！我是" } }],
              metadata: { author: "assistant" },
            },
            append: true,
            lastChunk: false,
          },
        },
      },
      2,
    );
    view = applyStreamEvent(
      view,
      {
        payload: {
          $case: "artifactUpdate",
          value: {
            artifact: {
              artifactId: id,
              parts: [{ content: { $case: "text", value: "规划大脑" } }],
              metadata: { author: "assistant" },
            },
            append: true,
            lastChunk: true,
          },
        },
      },
      3,
    );
    const msg = view.messages.find((m) => m.id === id);
    expect(msg?.text).toBe("你好！我是规划大脑");
    expect(msg?.thinking).toBe(false);
    expect(view.activeArtifactIds.has(id)).toBe(false);
    expect(view.workingBubbles).toHaveLength(0);
  });

  // --- snapshot artifact replay (resubscribe gap fill) ---

  function artifactInSnapshot(
    artifactId: string,
    parts: ProtoStruct[],
    metadata: ProtoStruct = {},
  ): ProtoStruct {
    return { artifactId, parts, metadata };
  }

  function taskSnapshot(
    artifacts: ProtoStruct[] = [],
    history: ProtoStruct[] = [],
    state: TaskState = TaskState.TASK_STATE_WORKING,
  ): ProtoStruct {
    return {
      payload: {
        $case: "task",
        value: {
          id: "task-1",
          status: { state },
          history,
          artifacts,
        },
      },
    };
  }

  function fcArtifact(
    name: string,
    args: ProtoStruct = {},
    result: ProtoStruct = { success: true },
    artifactId = `fc-${name}`,
  ): ProtoStruct {
    return artifactInSnapshot(
      artifactId,
      [{
        content: {
          $case: "data",
          value: { function_name: name, function_args: args, function_result: result },
        },
        metadata: { cw_type: "function_call" },
      }],
    );
  }

  it("snapshot replays unseen call_subagent dispatch artifact as a bubble", () => {
    const artifacts = [
      fcArtifact(
        "call_subagent",
        { requested_by: "assistant", target_agent: "echo", instruction: "任务甲" },
        { success: true },
      ),
    ];
    const view = applyStreamEvent(emptyConversation, taskSnapshot(artifacts), 1);
    expect(view.messages.some((m) => m.group === "dispatch" && m.text.includes("@echo"))).toBe(true);
    expect(view.seenArtifactIds.has("fc-call_subagent")).toBe(true);
  });

  it("snapshot replays unseen agent output artifact as an agent message", () => {
    const artifacts = [
      artifactInSnapshot(
        "art-node-1",
        [{ content: { $case: "text", value: "调研完成" } }],
        { node_id: "n1", author: "echo" },
      ),
    ];
    const view = applyStreamEvent(emptyConversation, taskSnapshot(artifacts), 1);
    const msg = view.messages.find((m) => m.id === "art-node-1");
    expect(msg?.role).toBe("agent");
    expect(msg?.text).toBe("调研完成");
    expect(msg?.node_id).toBe("n1");
    expect(msg?.sender).toBe("echo");
  });

  it("snapshot replays unseen thought artifact as a thinking message", () => {
    const artifacts = [
      artifactInSnapshot(
        "thought-1",
        [{ content: { $case: "text", value: "正在思考" }, metadata: { cw_thought: true } }],
        { author: "assistant" },
      ),
    ];
    const view = applyStreamEvent(emptyConversation, taskSnapshot(artifacts), 1);
    const msg = view.messages.find((m) => m.id === "thought-1");
    expect(msg?.thinking).toBe(true);
    expect(msg?.text).toBe("正在思考");
  });

  it("snapshot merges multi-part text artifact into single text", () => {
    const artifacts = [
      artifactInSnapshot(
        "art-multi",
        [
          { content: { $case: "text", value: "Hello" } },
          { content: { $case: "text", value: " World" } },
        ],
        { node_id: "n1", author: "echo" },
      ),
    ];
    const view = applyStreamEvent(emptyConversation, taskSnapshot(artifacts), 1);
    expect(view.messages.find((m) => m.id === "art-multi")?.text).toBe("Hello World");
  });

  it("snapshot skips artifacts already seen via live stream", () => {
    let view = applyStreamEvent(
      emptyConversation,
      functionCallEvent("create_plan", {
        nodes: [{ id: "n1", name: "task1", agent_name: "echo", deps: [], input: { text: "任务一" } }],
      }),
      1,
    );
    expect(view.nodes).toHaveLength(1);
    expect(view.nodes[0].input_text).toBe("任务一");
    expect(view.notifications.some((n) => n.kind === "plan.created")).toBe(true);

    const snapshotArtifacts = [
      fcArtifact(
        "create_plan",
        { nodes: [{ id: "n1", name: "task1", agent_name: "echo", deps: [], input: { text: "任务一" } }] },
        { success: true },
      ),
      fcArtifact(
        "call_subagent",
        { requested_by: "assistant", target_agent: "echo", instruction: "任务甲" },
        { success: true },
      ),
    ];
    view = applyStreamEvent(view, taskSnapshot(snapshotArtifacts), 2);
    expect(view.nodes).toHaveLength(1);
    expect(view.nodes[0].status).toBe("pending");
    expect(view.notifications.filter((n) => n.kind === "plan.created")).toHaveLength(1);
    expect(view.messages.some((m) => m.group === "dispatch")).toBe(true);
  });

  it("same snapshot applied twice does not duplicate", () => {
    const artifacts = [
      fcArtifact(
        "call_subagent",
        { requested_by: "assistant", target_agent: "echo", instruction: "任务甲" },
        { success: true },
      ),
      artifactInSnapshot(
        "art-node-1",
        [{ content: { $case: "text", value: "完成" } }],
        { node_id: "n1", author: "echo" },
      ),
    ];
    const snap = taskSnapshot(artifacts);
    let view = applyStreamEvent(emptyConversation, snap, 1);
    const msgCount = view.messages.length;
    view = applyStreamEvent(view, snap, 2);
    expect(view.messages.length).toBe(msgCount);
  });
});

describe("mergeConsecutiveJoins", () => {
  const join = (name: string, seq: number): TimelineItem => ({
    type: "notification",
    seq,
    data: {
      id: `sys-join-${name}-${seq}`,
      kind: "room.participant_joined",
      text: `${name} 加入了群聊`,
      agent_name: name,
      created_at: "",
      seq,
    },
  });

function itemText(item: TimelineItem): string {
  return item.type === "question" ? item.data.question : item.data.text;
}

  const userMsg = (seq: number): TimelineItem => ({
    type: "message",
    seq,
    data: {
      id: `m${seq}`,
      role: "user",
      sender: null,
      text: "hi",
      mentions: [],
      quote_id: null,
      node_id: null,
      task_id: null,
      created_at: "",
      seq,
      thinking: false,
    },
  });

  it("merges a batch run of join notifications into one line", () => {
    const merged = mergeConsecutiveJoins([
      join("product-manager", 1),
      join("developer", 2),
      join("code-reviewer", 3),
      join("qa-engineer", 4),
    ]);
    expect(merged).toHaveLength(1);
    expect(merged[0].seq).toBe(1);
    expect(itemText(merged[0])).toBe(
      "product-manager、developer、code-reviewer、qa-engineer 加入了群聊",
    );
  });

  it("merges adjacent joins across different events", () => {
    const merged = mergeConsecutiveJoins([join("a", 1), join("b", 5)]);
    expect(merged).toHaveLength(1);
    expect(itemText(merged[0])).toBe("a、b 加入了群聊");
  });

  it("keeps a single join notification untouched", () => {
    const merged = mergeConsecutiveJoins([join("a", 1)]);
    expect(merged).toHaveLength(1);
    expect(itemText(merged[0])).toBe("a 加入了群聊");
  });

  it("splits runs interrupted by other timeline items", () => {
    const merged = mergeConsecutiveJoins([
      join("a", 1),
      join("b", 2),
      userMsg(3),
      join("c", 4),
      join("d", 5),
    ]);
    expect(merged).toHaveLength(3);
    expect(itemText(merged[0])).toBe("a、b 加入了群聊");
    expect(merged[1].type).toBe("message");
    expect(itemText(merged[2])).toBe("c、d 加入了群聊");
  });
});

describe("questions", () => {
  function questionDataPart(id: string, overrides: Record<string, unknown> = {}) {
    return {
      content: {
        $case: "data",
        value: {
          intervention_id: id,
          node_id: "n1",
          requester: "writer",
          kind: "question",
          question_type: "select",
          options: ["A", "B"],
          multi: false,
          question: "选一个？",
          ...overrides,
        },
      },
      metadata: { cw_type: "question" },
    };
  }

  function questionStatusEvent(parts: unknown[]) {
    return {
      payload: {
        $case: "statusUpdate",
        value: {
          status: {
            state: TaskState.TASK_STATE_INPUT_REQUIRED,
            message: { parts },
          },
        },
      },
    };
  }

  it("creates a question from an input-required question message", () => {
    const view = applyStreamEvent(
      emptyConversation,
      questionStatusEvent([
        { content: { $case: "text", value: "选一个？" } },
        questionDataPart("iv1"),
      ]),
      1,
    );

    const question = view.questions.iv1;
    expect(question).toBeDefined();
    expect(question.question_type).toBe("select");
    expect(question.options).toEqual(["A", "B"]);
    expect(question.requester).toBe("writer");
    expect(question.status).toBe("pending");
    expect(question.seq).toBe(1);
    expect(view.state).toBe(taskStateToJSON(TaskState.TASK_STATE_INPUT_REQUIRED));
  });

  it("renders the planning layer as 规划大脑 for assistant/empty requesters", () => {
    const fromAssistant = applyStreamEvent(
      emptyConversation,
      questionStatusEvent([questionDataPart("iv1", { requester: "assistant" })]),
      1,
    );
    expect(fromAssistant.questions.iv1.requester).toBe("规划大脑");

    const fromEmpty = applyStreamEvent(
      emptyConversation,
      questionStatusEvent([questionDataPart("iv2", { requester: "" })]),
      2,
    );
    expect(fromEmpty.questions.iv2.requester).toBe("规划大脑");
  });

  it("marks a question resolved from a state_delta answer", () => {
    let view = applyStreamEvent(
      emptyConversation,
      questionStatusEvent([questionDataPart("iv1")]),
      1,
    );
    view = applyStreamEvent(
      view,
      statusUpdate("state_delta", {
        interventions: {
          iv1: {
            status: "resolved",
            node_id: "n1",
            kind: "question",
            answer: "A",
            responder: "human",
          },
        },
      }),
      2,
    );

    expect(view.questions.iv1.status).toBe("resolved");
    expect(view.questions.iv1.answer).toBe("A");
  });

  it("marks a question rejected on intervention.rejected", () => {
    let view = applyStreamEvent(
      emptyConversation,
      questionStatusEvent([questionDataPart("iv1")]),
      1,
    );
    view = applyStreamEvent(
      view,
      statusUpdate("intervention.rejected", { intervention_id: "iv1", reason: "bad answer" }),
      2,
    );

    expect(view.questions.iv1.status).toBe("rejected");
    expect(view.questions.iv1.error).toBe("bad answer");
  });

  it("rebuilds questions from task history without duplicating", () => {
    const task = {
      payload: {
        $case: "task",
        value: {
          id: "t1",
          contextId: "c1",
          status: { state: TaskState.TASK_STATE_INPUT_REQUIRED },
          history: [
            {
              messageId: "m1",
              role: 2,
              parts: [
                { content: { $case: "text", value: "选一个？" } },
                questionDataPart("iv1"),
              ],
            },
          ],
        },
      },
    };

    let view = applyStreamEvent(emptyConversation, task, 1);
    view = applyStreamEvent(view, task, 2);

    expect(Object.keys(view.questions)).toEqual(["iv1"]);
    expect(view.messages.some((message) => message.id === "m1")).toBe(false);
  });
});
