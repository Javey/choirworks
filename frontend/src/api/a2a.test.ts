import { afterEach, describe, expect, it, vi } from "vitest";

import {
  A2AError,
  A2A_ROOM_URI,
  postJson,
  postSse,
  roomEventFromResult,
  roomSnapshotFromTask,
  sendMessageParams,
  subscribeRoom,
  subscribeTask,
  taskEventFromResult,
  taskEventsFromSnapshot,
} from "./a2a";

const encoder = new TextEncoder();

function sseResponse(chunks: string[], { close = true } = {}): Response {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      if (close) controller.close();
    },
  });
  return new Response(body, {
    status: 200,
    headers: { "content-type": "text/event-stream; charset=utf-8" },
  });
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("postSse", () => {
  it("parses data frames, multi-line data, comments and CRLF", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        sseResponse([
          'data: {"result":{"task":{"id":"c1"}},"id":1,"jsonrpc":"2.0"}\r\n\r\n',
          ": ping\r\n\r\n",
          "data: {\r\n",
          'data: "result":{"message":{"messageId":"m1"}},"id":1,"jsonrpc":"2.0"}\r\n\r\n',
        ]),
      ),
    );
    const frames = [];
    for await (const frame of postSse("/v1/a2a", { any: true })) frames.push(frame);
    expect(frames).toHaveLength(2);
    expect(frames[0].result?.task).toEqual({ id: "c1" });
    expect(frames[1].result?.message).toEqual({ messageId: "m1" });
  });

  it("handles frames split across network chunks and keeps stream open", async () => {
    const controllerRef: {
      controller?: ReadableStreamDefaultController<Uint8Array>;
    } = {};
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controllerRef.controller = controller;
        controller.enqueue(encoder.encode('data: {"result":{"task":{"id":"c'));
      },
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(body, {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          }),
      ),
    );
    const iterator = postSse("/v1/a2a", {});
    const next = iterator.next();
    controllerRef.controller?.enqueue(
      encoder.encode('2"}},"id":1,"jsonrpc":"2.0"}\n\n'),
    );
    const frame = await next;
    expect(frame.done).toBe(false);
    expect(frame.value.result?.task).toEqual({ id: "c2" });
    await iterator.return(undefined);
  });

  it("sends JSON-RPC request with A2A-Version header", async () => {
    const fetchMock = vi.fn(async () => sseResponse([]));
    vi.stubGlobal("fetch", fetchMock);
    for await (const frame of postSse("/v1/a2a", { jsonrpc: "2.0", id: 7 })) {
      void frame;
    }
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/v1/a2a");
    expect((init.headers as Record<string, string>)["A2A-Version"]).toBe("1.0");
    expect(JSON.parse(String(init.body)).id).toBe(7);
  });

  it("yields JSON-RPC error frames", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        sseResponse([
          'event: error\ndata: {"error":{"code":-32001,"message":"task not found: missing"},"id":3,"jsonrpc":"2.0"}\n\n',
        ]),
      ),
    );
    const frames = [];
    for await (const frame of postSse("/v1/a2a", {})) frames.push(frame);
    expect(frames[0].error?.code).toBe(-32001);
  });

  it("yields JSON body when server answers non-SSE", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        jsonResponse({
          error: { code: -32009, message: "A2A version '0.3' is not supported" },
          id: 1,
          jsonrpc: "2.0",
        }),
      ),
    );
    const frames = [];
    for await (const frame of postSse("/v1/a2a", {})) frames.push(frame);
    expect(frames[0].error?.code).toBe(-32009);
  });
});

describe("postJson", () => {
  it("returns result and throws A2AError on JSON-RPC error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        jsonResponse({ jsonrpc: "2.0", id: 1, result: { task: { id: "t1" } } }),
      ),
    );
    await expect(postJson("/v1/a2a", {})).resolves.toEqual({ task: { id: "t1" } });

    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        jsonResponse({
          jsonrpc: "2.0",
          id: 2,
          error: { code: -32001, message: "not found" },
        }),
      ),
    );
    const error = await postJson("/v1/a2a", {}).catch((exc: unknown) => exc);
    expect(error).toBeInstanceOf(A2AError);
    expect((error as A2AError).code).toBe(-32001);
  });
});

const ROOM_TASK_FRAME = {
  id: "c1",
  contextId: "c1",
  status: { state: "TASK_STATE_INPUT_REQUIRED" },
  history: [
    {
      messageId: "m1",
      contextId: "c1",
      taskId: "c1",
      role: "ROLE_USER",
      parts: [{ text: "大家好" }],
      metadata: {
        [A2A_ROOM_URI]: {
          kind: "message",
          sender: "CEO",
          seq: 1,
          mentions: [],
          quote_id: null,
          node_id: null,
        },
      },
    },
  ],
  metadata: {
    [A2A_ROOM_URI]: {
      kind: "room",
      title: "测试群",
      members: [
        {
          agent_name: "echo",
          agent_url: "http://agent",
          reason: "human_mention",
          joined_at: "2026-09-14T00:00:00+00:00",
        },
      ],
      summary: {
        covers_seq: 1,
        updated_at: "2026-09-14T00:00:00+00:00",
        content: { topics: ["问候"] },
      },
      message_count: 1,
      last_seq: 1,
    },
  },
};

describe("roomSnapshotFromTask", () => {
  it("rebuilds RoomMessagesDto from synthetic room task", () => {
    const snapshot = roomSnapshotFromTask(ROOM_TASK_FRAME);
    expect(snapshot.last_seq).toBe(1);
    expect(snapshot.messages[0].id).toBe("m1");
    expect(snapshot.messages[0].role).toBe("user");
    expect(snapshot.messages[0].seq).toBe(1);
    expect(snapshot.members[0].agent_name).toBe("echo");
    expect(snapshot.summary?.covers_seq).toBe(1);
    expect(snapshot.summary?.summary).toEqual({ topics: ["问候"] });
  });

  it("handles a room without summary", () => {
    const task = {
      ...ROOM_TASK_FRAME,
      metadata: {
        [A2A_ROOM_URI]: {
          ...ROOM_TASK_FRAME.metadata[A2A_ROOM_URI],
          summary: {},
        },
      },
    };
    expect(roomSnapshotFromTask(task).summary).toBeNull();
  });
});

describe("roomEventFromResult", () => {
  it("maps message frames to message.posted", () => {
    const event = roomEventFromResult({
      message: {
        messageId: "m9",
        contextId: "c1",
        taskId: "t1",
        role: "ROLE_AGENT",
        parts: [{ text: "直播消息" }],
        metadata: {
          [A2A_ROOM_URI]: {
            kind: "message",
            sender: "echo",
            seq: 9,
            mentions: ["writer"],
            quote_id: "m1",
            node_id: "p1:n1",
            queued_for_node_id: null,
          },
        },
      },
    });
    expect(event?.type).toBe("message.posted");
    expect(event?.seq).toBe(9);
    expect(event?.payload).toMatchObject({
      message_id: "m9",
      role: "agent",
      sender: "echo",
      text: "直播消息",
      mentions: ["writer"],
      quote_id: "m1",
      node_id: "p1:n1",
    });
  });

  it("maps room status updates by metadata kind", () => {
    const delivered = roomEventFromResult({
      statusUpdate: {
        contextId: "c1",
        status: { state: "TASK_STATE_WORKING" },
        metadata: {
          [A2A_ROOM_URI]: {
            kind: "message.delivered",
            message_id: "m1",
            node_id: "p1:n1",
          },
        },
      },
    });
    expect(delivered?.type).toBe("message.delivered");

    const joined = roomEventFromResult({
      statusUpdate: {
        contextId: "c1",
        status: { state: "TASK_STATE_INPUT_REQUIRED" },
        metadata: {
          [A2A_ROOM_URI]: {
            kind: "room.participant_joined",
            agent_name: "echo",
            agent_url: "http://a",
            reason: "human_mention",
          },
        },
      },
    });
    expect(joined?.type).toBe("room.participant_joined");
    expect(joined?.payload.agent_name).toBe("echo");

    const summary = roomEventFromResult({
      statusUpdate: {
        contextId: "c1",
        status: { state: "TASK_STATE_INPUT_REQUIRED" },
        metadata: {
          [A2A_ROOM_URI]: {
            kind: "room.summary_updated",
            covers_seq: 5,
            summary: { topics: ["x"] },
          },
        },
      },
    });
    expect(summary?.type).toBe("room.summary_updated");
    expect(summary?.payload.covers_seq).toBe(5);
  });

  it("ignores task events and room tasks", () => {
    expect(
      roomEventFromResult({
        statusUpdate: {
          contextId: "c1",
          status: { state: "TASK_STATE_WORKING" },
          metadata: { kind: "node.state_changed", node_id: "n1" },
        },
      }),
    ).toBeNull();
    expect(roomEventFromResult({ task: { id: "c1" } })).toBeNull();
  });
});

describe("sendMessageParams", () => {
  it("builds message with contextId and room metadata", () => {
    const params = sendMessageParams({
      text: "请处理",
      contextId: "c1",
      roomMeta: { mentions: ["echo"], quote_id: "m1", interrupt: true },
    }) as { message: Record<string, unknown> };
    expect(params.message.contextId).toBe("c1");
    expect(params.message.role).toBe("ROLE_USER");
    expect(params.message.parts).toEqual([{ text: "请处理" }]);
    expect(params.message.metadata).toEqual({
      [A2A_ROOM_URI]: { mentions: ["echo"], quote_id: "m1", interrupt: true },
    });
    expect(String(params.message.messageId)).toMatch(/^web-/);
  });

  it("omits empty options", () => {
    const params = sendMessageParams({ text: "hi" }) as {
      message: Record<string, unknown>;
    };
    expect("contextId" in params.message).toBe(false);
    expect("taskId" in params.message).toBe(false);
    expect("metadata" in params.message).toBe(false);
  });
});

describe("task adapters", () => {
  it("maps statusUpdate kinds and task states", () => {
    const node = taskEventFromResult(
      {
        statusUpdate: {
          status: { state: "TASK_STATE_WORKING" },
          metadata: {
            kind: "node.state_changed",
            node_id: "p1:n1",
            from: "dispatched",
            to: "working",
          },
        },
      },
      3,
    );
    expect(node?.type).toBe("node.state_changed");
    expect(node?.payload.to).toBe("working");

    const completed = taskEventFromResult(
      {
        statusUpdate: {
          status: { state: "TASK_STATE_COMPLETED" },
          metadata: {},
        },
      },
      4,
    );
    expect(completed?.type).toBe("task.completed");

    const failed = taskEventFromResult(
      {
        statusUpdate: { status: { state: "TASK_STATE_FAILED" }, metadata: {} },
      },
      5,
    );
    expect(failed?.type).toBe("task.failed");
  });

  it("maps artifact updates to node.artifact with id prefix", () => {
    const event = taskEventFromResult(
      {
        artifactUpdate: {
          artifact: {
            artifactId: "plan1:n1:remote-1",
            parts: [{ text: "echo:hi" }],
          },
          append: false,
        },
      },
      2,
    );
    expect(event?.type).toBe("node.artifact");
    expect(event?.payload).toMatchObject({
      node_id: "plan1:n1",
      text: "echo:hi",
      append: false,
    });

    const lastChunk = taskEventFromResult(
      {
        artifactUpdate: {
          artifact: { artifactId: "plan1:n1:remote-1" },
          append: true,
          lastChunk: true,
        },
      },
      3,
    );
    expect(lastChunk).toBeNull();
  });

  it("maps plan artifact updates with dag payload", () => {
    const event = taskEventFromResult(
      {
        artifactUpdate: {
          artifact: {
            artifactId: "plan:plan1",
            parts: [{ data: { nodes: [{ id: "n1", name: "step" }] } }],
          },
          metadata: { kind: "plan.created", plan_id: "plan1", version: 1 },
        },
      },
      1,
    );
    expect(event?.type).toBe("plan.created");
    expect(event?.payload.dag).toEqual({ nodes: [{ id: "n1", name: "step" }] });
  });

  it("expands a task snapshot into events", () => {
    const events = taskEventsFromSnapshot(
      {
        id: "t1",
        status: { state: "TASK_STATE_WORKING" },
        metadata: {
          nodes: [
            { id: "p1:n1", status: "working", agent_name: "echo", attempt: 1 },
          ],
        },
        artifacts: [
          {
            artifactId: "p1:n1:r1",
            parts: [{ text: "done" }],
          },
        ],
      },
      10,
    );
    expect(events[0].type).toBe("task.state_changed");
    expect(events[0].payload.to).toBe("running");
    expect(events[1].type).toBe("node.state_changed");
    expect(events[1].payload.node_id).toBe("p1:n1");
    expect(events[2].type).toBe("node.artifact");
    expect(events.map((event) => event.seq)).toEqual([11, 12, 13]);
  });
});

describe("subscriptions", () => {
  it("subscribes room: snapshot first, then live message, then reconnects", async () => {
    const frames = [
      `data: ${JSON.stringify({ result: { task: ROOM_TASK_FRAME }, id: 1, jsonrpc: "2.0" })}\n\n`,
      `data: ${JSON.stringify({
        result: {
          message: {
            messageId: "m2",
            contextId: "c1",
            taskId: "c1",
            role: "ROLE_USER",
            parts: [{ text: "live" }],
            metadata: {
              [A2A_ROOM_URI]: {
                kind: "message",
                sender: "CEO",
                seq: 2,
                mentions: [],
              },
            },
          },
        },
        id: 1,
        jsonrpc: "2.0",
      })}\n\n`,
    ];
    let calls = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        calls += 1;
        if (calls === 1) return sseResponse(frames);
        return sseResponse([]);
      }),
    );
    const snapshots: unknown[] = [];
    const events: unknown[] = [];
    const states: string[] = [];
    const close = subscribeRoom(
      "c1",
      {
        onSnapshot: (snapshot) => snapshots.push(snapshot),
        onEvent: (event) => events.push(event),
        onState: (state) => states.push(state),
      },
      { retryDelayMs: 1 },
    );
    await vi.waitFor(() => expect(events).toHaveLength(1));
    await vi.waitFor(() => expect(snapshots).toHaveLength(1));
    await vi.waitFor(() => expect(states).toContain("reconnecting"));
    close();
    expect(events[0]).toMatchObject({ type: "message.posted" });
  });

  it("subscribes task with incrementing seq", async () => {
    const frames = [
      `data: ${JSON.stringify({
        result: {
          statusUpdate: {
            status: { state: "TASK_STATE_WORKING" },
            metadata: { kind: "node.dispatched", node_id: "p1:n1" },
          },
        },
        id: 1,
        jsonrpc: "2.0",
      })}\n\n`,
    ];
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => sseResponse(frames, { close: false })),
    );
    const events: { seq: number; type: string }[] = [];
    const close = subscribeTask(
      "t1",
      {
        onEvent: (event) => events.push({ seq: event.seq, type: event.type }),
        onState: () => undefined,
      },
      { baseSeq: 4, retryDelayMs: 1 },
    );
    await vi.waitFor(() => expect(events).toHaveLength(1));
    close();
    expect(events[0]).toEqual({ seq: 5, type: "node.dispatched" });
  });

  it("surfaces JSON-RPC errors as reconnecting state", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            JSON.stringify({
              error: { code: -32001, message: "task not found" },
              id: 1,
              jsonrpc: "2.0",
            }),
            { status: 200, headers: { "content-type": "application/json" } },
          ),
      ),
    );
    const states: string[] = [];
    const close = subscribeTask(
      "missing",
      {
        onEvent: () => undefined,
        onState: (state) => states.push(state),
      },
      { retryDelayMs: 1 },
    );
    await vi.waitFor(() => expect(states).toContain("reconnecting"));
    close();
  });
});
