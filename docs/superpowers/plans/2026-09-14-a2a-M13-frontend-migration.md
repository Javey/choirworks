# M13 Frontend A2A Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 前端订阅与发送从「REST + EventSource」迁移到 A2A JSON-RPC over POST SSE，UI view-model 与组件零改动；REST 快照保留。

**Architecture:** 新增 `frontend/src/api/a2a.ts`：低层 `postSse`（W3C SSE 解析 + `A2A-Version: 1.0` 头）+ `postJson`；适配层把 `StreamResponse`（camelCase proto JSON）还原成现有 `EventDto` / `RoomMessagesDto`，`useRoom` / `useConversation` / `App.send` 换用适配层，`applyRoomEvent` / `applyEvent` / `mergeRoomSnapshot` 等 view-model 逻辑全部复用。

**Tech Stack:** React 19 / TypeScript / Vite 8 / Vitest 5 + jsdom / oxlint；后端无需改动（M11/M12 已完成）。

**Spec:** `docs/superpowers/specs/2026-09-13-a2a-facade-design.md` §8、§9 M13、§10（前端测试）。

**Wire 事实（实测抓帧，2026-09-14）：**
- 所有请求需 `A2A-Version: 1.0` 头，否则 `-32009 VERSION_NOT_SUPPORTED`。
- 流式帧：`data: {json}\n\n`；错误帧额外有 `event: error`，`data` 内为 `{"error":{...}}`。
- `result` 为 camelCase proto JSON：oneof 键 `task` / `message` / `statusUpdate` / `artifactUpdate`。
- Room metadata URI = `https://github.com/Javey/choirworks/extensions/room/v1`；`statusUpdate.metadata[room-uri].kind` ∈ `message.delivered` / `room.participant_joined` / `room.summary_updated`。
- 任务 `statusUpdate.metadata.kind` = 内部事件名（`node.state_changed` 等，payload 为 snake_case 键）；任务 artifact id = `{plan_id}:{dag_node_id}:{remote_id}`；`lastChunk` 帧无 parts。

---

## 文件结构

| 文件 | 职责 | 动作 |
|---|---|---|
| `frontend/src/api/a2a.ts` | POST SSE 解析 + JSON-RPC + A2A↔view-model 适配 | 新建 |
| `frontend/src/api/a2a.test.ts` | 解析器与适配层单测（真实抓帧夹具） | 新建 |
| `frontend/src/hooks/useRoom.ts` | 订阅/发送切 A2A，REST 快照保留 | 修改 |
| `frontend/src/hooks/useConversation.ts` | 任务订阅切 A2A | 修改 |
| `frontend/src/App.tsx` | 首条消息发送切 A2A | 修改 |
| `frontend/src/App.test.tsx` | fetch mock 增补 A2A 端点 | 修改 |

命令均在 `frontend/` 下执行：`npm test`（vitest run）、`npm run build`（tsc -b + vite build）、`npm run lint`（oxlint）。

---

### Task 1: `postSse` 与 `postJson` 低层

**Files:**
- Create: `frontend/src/api/a2a.ts`
- Test: `frontend/src/api/a2a.test.ts`

- [ ] **Step 1: 写失败测试**

新建 `frontend/src/api/a2a.test.ts`：

```ts
import { afterEach, describe, expect, it, vi } from "vitest";

import { A2AError, postJson, postSse } from "./a2a";

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
    const controllerRef: { controller?: ReadableStreamDefaultController<Uint8Array> } = {};
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controllerRef.controller = controller;
        controller.enqueue(encoder.encode('data: {"result":{"task":{"id":"c'));
      },
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } })),
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
    for await (const _ of postSse("/v1/a2a", { jsonrpc: "2.0", id: 7 })) void _;
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
        jsonResponse({ jsonrpc: "2.0", id: 2, error: { code: -32001, message: "not found" } }),
      ),
    );
    const error = await postJson("/v1/a2a", {}).catch((exc: unknown) => exc);
    expect(error).toBeInstanceOf(A2AError);
    expect((error as A2AError).code).toBe(-32001);
  });
});
```

- [ ] **Step 2: 运行测试确认失败**

Run: `npm test -- src/api/a2a.test.ts`
Expected: FAIL（`Failed to resolve import "./a2a"`）

- [ ] **Step 3: 实现低层**

新建 `frontend/src/api/a2a.ts`：

```ts
export const A2A_URL = "/v1/a2a";
export const A2A_ROOM_URI =
  "https://github.com/Javey/choirworks/extensions/room/v1";

export class A2AError extends Error {
  code: number;

  constructor(code: number, message: string) {
    super(message);
    this.name = "A2AError";
    this.code = code;
  }
}

export interface JsonRpcResponse {
  jsonrpc: string;
  id: number | string;
  result?: Record<string, unknown>;
  error?: { code?: number; message?: string };
}

let nextRequestId = 1;

export function rpcRequest(
  method: string,
  params: Record<string, unknown>,
): Record<string, unknown> {
  return { jsonrpc: "2.0", id: nextRequestId++, method, params };
}

function requestHeaders(): Record<string, string> {
  return { "content-type": "application/json", "A2A-Version": "1.0" };
}

function drainFrames(raw: string): { payloads: string[]; rest: string } {
  const normalized = raw.replace(/\r\n/g, "\n");
  const blocks = normalized.split("\n\n");
  const rest = blocks.pop() ?? "";
  const payloads: string[] = [];
  for (const block of blocks) {
    const dataLines = block
      .split("\n")
      .filter((line) => line.startsWith("data:"));
    if (dataLines.length === 0) continue;
    payloads.push(dataLines.map((line) => line.slice(5).trimStart()).join("\n"));
  }
  return { payloads, rest };
}

function parseBody(text: string): JsonRpcResponse | null {
  try {
    return text ? (JSON.parse(text) as JsonRpcResponse) : null;
  } catch {
    return null;
  }
}

export async function postJson(
  url: string,
  body: unknown,
): Promise<Record<string, unknown>> {
  const response = await fetch(url, {
    method: "POST",
    headers: requestHeaders(),
    body: JSON.stringify(body),
  });
  const data = parseBody(await response.text());
  if (data?.error) {
    throw new A2AError(
      data.error.code ?? -32000,
      data.error.message ?? "A2A 请求失败",
    );
  }
  if (!response.ok || data === null) {
    throw new A2AError(response.status, `请求失败（HTTP ${response.status}）`);
  }
  return data.result ?? {};
}

export async function* postSse(
  url: string,
  body: unknown,
  signal?: AbortSignal,
): AsyncGenerator<JsonRpcResponse> {
  const response = await fetch(url, {
    method: "POST",
    headers: { ...requestHeaders(), accept: "text/event-stream" },
    body: JSON.stringify(body),
    signal,
  });
  const contentType = response.headers.get("content-type") ?? "";
  if (!contentType.includes("text/event-stream") || !response.body) {
    const data = parseBody(await response.text());
    if (data) {
      yield data;
      return;
    }
    throw new A2AError(response.status, `请求失败（HTTP ${response.status}）`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { done, value } = await reader.read();
    if (value) buffer += decoder.decode(value, { stream: true });
    const { payloads, rest } = drainFrames(buffer);
    buffer = rest;
    for (const payload of payloads) {
      const data = parseBody(payload);
      if (data) yield data;
    }
    if (done) break;
  }
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `npm test -- src/api/a2a.test.ts`
Expected: PASS（6 tests）

- [ ] **Step 5: 提交**

```bash
git add frontend/src/api/a2a.ts frontend/src/api/a2a.test.ts
git commit -m "feat(web): A2A POST SSE 解析与 JSON-RPC 客户端（M13）"
```

---

### Task 2: Room 适配层

**Files:**
- Modify: `frontend/src/api/a2a.ts`
- Test: `frontend/src/api/a2a.test.ts`

- [ ] **Step 1: 写失败测试**

在 `frontend/src/api/a2a.test.ts` 的 import 中加入 `roomEventFromResult`、`roomSnapshotFromTask`、`sendMessageParams`，并追加：

```ts
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
        [A2A_ROOM_URI]: { ...ROOM_TASK_FRAME.metadata[A2A_ROOM_URI], summary: {} },
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
        metadata: { [A2A_ROOM_URI]: { kind: "message.delivered", message_id: "m1", node_id: "p1:n1" } },
      },
    });
    expect(delivered?.type).toBe("message.delivered");

    const joined = roomEventFromResult({
      statusUpdate: {
        contextId: "c1",
        status: { state: "TASK_STATE_INPUT_REQUIRED" },
        metadata: { [A2A_ROOM_URI]: { kind: "room.participant_joined", agent_name: "echo", agent_url: "http://a", reason: "human_mention" } },
      },
    });
    expect(joined?.type).toBe("room.participant_joined");
    expect(joined?.payload.agent_name).toBe("echo");

    const summary = roomEventFromResult({
      statusUpdate: {
        contextId: "c1",
        status: { state: "TASK_STATE_INPUT_REQUIRED" },
        metadata: { [A2A_ROOM_URI]: { kind: "room.summary_updated", covers_seq: 5, summary: { topics: ["x"] } } },
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
```

import 行改为：

```ts
import {
  A2AError,
  A2A_ROOM_URI,
  postJson,
  postSse,
  roomEventFromResult,
  roomSnapshotFromTask,
  sendMessageParams,
} from "./a2a";
```

- [ ] **Step 2: 运行测试确认失败**

Run: `npm test -- src/api/a2a.test.ts`
Expected: FAIL（导出不存在）

- [ ] **Step 3: 实现 Room 适配**

在 `frontend/src/api/a2a.ts` 追加（`import type { EventDto, PostMessageOutDto, RoomMessageDto, RoomMessagesDto } from "../lib/types";` 放文件顶部）：

```ts
type ProtoStruct = Record<string, unknown>;

function metadataOf(container: ProtoStruct): ProtoStruct {
  return (container.metadata as ProtoStruct | undefined) ?? {};
}

function roomMetaOf(container: ProtoStruct): ProtoStruct {
  return (metadataOf(container)[A2A_ROOM_URI] as ProtoStruct | undefined) ?? {};
}

function textOfParts(container: ProtoStruct): string {
  const parts = (container.parts as { text?: string }[] | undefined) ?? [];
  return parts.map((part) => part.text ?? "").join("\n");
}

function roleFromA2A(role: unknown): RoomMessageDto["role"] {
  return role === "ROLE_AGENT" ? "agent" : "user";
}

function messageFromA2A(message: ProtoStruct): RoomMessageDto {
  const room = roomMetaOf(message);
  return {
    id: String(message.messageId ?? ""),
    conversation_id: String(message.contextId ?? ""),
    seq: Number(room.seq ?? 0),
    role: roleFromA2A(message.role),
    sender: typeof room.sender === "string" ? room.sender : null,
    text: textOfParts(message),
    mentions: Array.isArray(room.mentions) ? room.mentions.map(String) : [],
    quote_id: typeof room.quote_id === "string" ? room.quote_id : null,
    task_id: typeof message.taskId === "string" ? message.taskId : null,
    node_id: typeof room.node_id === "string" ? room.node_id : null,
    intervention_id:
      typeof room.intervention_id === "string" ? room.intervention_id : null,
    queued_for_node_id:
      typeof room.queued_for_node_id === "string"
        ? room.queued_for_node_id
        : null,
    delivered_at: null,
    created_at: new Date().toISOString(),
  };
}

export function roomSnapshotFromTask(task: ProtoStruct): RoomMessagesDto {
  const meta = roomMetaOf(task);
  const history = (task.history as ProtoStruct[] | undefined) ?? [];
  const messages = history
    .map(messageFromA2A)
    .sort((left, right) => left.seq - right.seq);
  const members = (
    (meta.members as {
      agent_name?: string;
      agent_url?: string;
      reason?: string | null;
      joined_at?: string;
    }[]) ?? []
  ).map((member) => ({
    conversation_id: String(task.id ?? ""),
    agent_name: String(member.agent_name ?? ""),
    agent_url: String(member.agent_url ?? ""),
    reason: member.reason ?? null,
    joined_at: member.joined_at ?? new Date().toISOString(),
  }));
  const summaryRaw = (meta.summary as ProtoStruct | undefined) ?? {};
  const hasSummary = Object.keys(summaryRaw).length > 0;
  return {
    messages,
    members,
    summary: hasSummary
      ? {
          conversation_id: String(task.id ?? ""),
          covers_seq: Number(summaryRaw.covers_seq ?? 0),
          summary: (summaryRaw.content as ProtoStruct | undefined) ?? {},
          updated_at: String(summaryRaw.updated_at ?? ""),
        }
      : null,
    last_seq: Number(meta.last_seq ?? messages.at(-1)?.seq ?? 0),
  };
}

export function roomEventFromResult(result: ProtoStruct): EventDto | null {
  if (result.message) {
    const message = messageFromA2A(result.message as ProtoStruct);
    return {
      seq: message.seq,
      type: "message.posted",
      payload: { ...message } as unknown as EventDto["payload"],
    };
  }
  const update = result.statusUpdate as ProtoStruct | undefined;
  if (!update) return null;
  const room = roomMetaOf(update);
  const contextId = String(update.contextId ?? "");
  if (room.kind === "message.delivered") {
    return {
      seq: 0,
      type: "message.delivered",
      payload: { message_id: room.message_id ?? null, node_id: room.node_id ?? null },
    };
  }
  if (room.kind === "room.participant_joined") {
    return {
      seq: 0,
      type: "room.participant_joined",
      payload: {
        conversation_id: contextId,
        agent_name: room.agent_name ?? "",
        agent_url: room.agent_url ?? "",
        reason: room.reason ?? null,
        joined_at: new Date().toISOString(),
      },
    };
  }
  if (room.kind === "room.summary_updated") {
    return {
      seq: 0,
      type: "room.summary_updated",
      payload: {
        conversation_id: contextId,
        covers_seq: room.covers_seq ?? 0,
        summary: room.summary ?? {},
      },
    };
  }
  return null;
}

export interface RoomSendInput {
  text: string;
  mentions?: string[];
  quote_id?: string;
  interrupt?: boolean;
}

function newMessageId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return `web-${crypto.randomUUID()}`;
  }
  return `web-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export function sendMessageParams(input: {
  text: string;
  contextId?: string;
  taskId?: string;
  roomMeta?: ProtoStruct;
}): ProtoStruct {
  const message: ProtoStruct = {
    messageId: newMessageId(),
    role: "ROLE_USER",
    parts: [{ text: input.text }],
  };
  if (input.contextId) message.contextId = input.contextId;
  if (input.taskId) message.taskId = input.taskId;
  if (input.roomMeta) message.metadata = { [A2A_ROOM_URI]: input.roomMeta };
  return { message };
}

export async function sendRoomMessage(
  conversationId: string,
  input: RoomSendInput,
): Promise<PostMessageOutDto | null> {
  const roomMeta: ProtoStruct = {};
  if (input.mentions && input.mentions.length > 0) roomMeta.mentions = input.mentions;
  if (input.quote_id) roomMeta.quote_id = input.quote_id;
  if (input.interrupt) roomMeta.interrupt = true;
  const result = await postJson(
    A2A_URL,
    rpcRequest(
      "SendMessage",
      sendMessageParams({
        text: input.text,
        contextId: conversationId,
        roomMeta,
      }),
    ),
  );
  const message = result.message as ProtoStruct | undefined;
  if (!message) return null;
  const room = roomMetaOf(message);
  return {
    message_id: String(message.messageId ?? ""),
    seq: Number(room.seq ?? 0),
    task_id: typeof message.taskId === "string" ? message.taskId : null,
  };
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `npm test -- src/api/a2a.test.ts`
Expected: PASS（15 tests）

- [ ] **Step 5: 提交**

```bash
git add frontend/src/api/a2a.ts frontend/src/api/a2a.test.ts
git commit -m "feat(web): Room 快照/事件/发送适配层（M13）"
```

---

### Task 3: Task 适配层与订阅（含断线重连）

**Files:**
- Modify: `frontend/src/api/a2a.ts`
- Test: `frontend/src/api/a2a.test.ts`

- [ ] **Step 1: 写失败测试**

在 `frontend/src/api/a2a.test.ts` 的 import 中加入 `subscribeRoom`、`subscribeTask`、`taskEventFromResult`、`taskEventsFromSnapshot`，并追加：

```ts
describe("task adapters", () => {
  it("maps statusUpdate kinds and task states", () => {
    const node = taskEventFromResult(
      {
        statusUpdate: {
          status: { state: "TASK_STATE_WORKING" },
          metadata: { kind: "node.state_changed", node_id: "p1:n1", from: "dispatched", to: "working" },
        },
      },
      3,
    );
    expect(node?.type).toBe("node.state_changed");
    expect(node?.payload.to).toBe("working");

    const completed = taskEventFromResult(
      { statusUpdate: { status: { state: "TASK_STATE_COMPLETED" }, metadata: {} } },
      4,
    );
    expect(completed?.type).toBe("task.completed");

    const failed = taskEventFromResult(
      { statusUpdate: { status: { state: "TASK_STATE_FAILED" }, metadata: {} } },
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
        status: { state: "TASK_STATE_RUNNING" },
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
            metadata: { [A2A_ROOM_URI]: { kind: "message", sender: "CEO", seq: 2, mentions: [] } },
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
        return sseResponse([]); // 第二次订阅保持打开
      }),
    );
    const snapshots: unknown[] = [];
    const events: unknown[] = [];
    const states: string[] = [];
    const close = subscribeRoom("c1", {
      onSnapshot: (snapshot) => snapshots.push(snapshot),
      onEvent: (event) => events.push(event),
      onState: (state) => states.push(state),
    }, { retryDelayMs: 1 });
    await vi.waitFor(() => expect(events).toHaveLength(1));
    await vi.waitFor(() => expect(snapshots).toHaveLength(1));
    await vi.waitFor(() => expect(states).toContain("reconnecting"));
    close();
    expect(events[0]).toMatchObject({ type: "message.posted" });
  });

  it("subscribes task with incrementing seq", async () => {
    const frames = [
      `data: ${JSON.stringify({
        result: { statusUpdate: { status: { state: "TASK_STATE_WORKING" }, metadata: { kind: "node.dispatched", node_id: "p1:n1" } } },
        id: 1,
        jsonrpc: "2.0",
      })}\n\n`,
    ];
    vi.stubGlobal("fetch", vi.fn(async () => sseResponse(frames)));
    const events: { seq: number; type: string }[] = [];
    const close = subscribeTask("t1", {
      onEvent: (event) => events.push({ seq: event.seq, type: event.type }),
      onState: () => undefined,
    }, { baseSeq: 4, retryDelayMs: 1 });
    await vi.waitFor(() => expect(events).toHaveLength(1));
    close();
    expect(events[0]).toEqual({ seq: 5, type: "node.dispatched" });
  });

  it("surfaces JSON-RPC errors as reconnecting state", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
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
    const close = subscribeTask("missing", {
      onEvent: () => undefined,
      onState: (state) => states.push(state),
    }, { retryDelayMs: 1 });
    await vi.waitFor(() => expect(states).toContain("reconnecting"));
    close();
  });
});
```

- [ ] **Step 2: 运行测试确认失败**

Run: `npm test -- src/api/a2a.test.ts`
Expected: FAIL（`subscribeRoom` / `subscribeTask` 导出不存在）

- [ ] **Step 3: 实现 Task 适配与订阅**

在 `frontend/src/api/a2a.ts` 追加（顶部 import type 增加 `TaskStatus`）：

```ts
export type ConnectionState = "live" | "reconnecting" | "closed";

export interface SubscriptionHandlers {
  onSnapshot?: (task: ProtoStruct) => void;
  onEvent: (event: EventDto) => void;
  onState: (state: ConnectionState) => void;
}

export interface SubscriptionOptions {
  baseSeq?: number;
  retryDelayMs?: number;
}

const TASK_STATE_TO_STATUS: Record<string, TaskStatus> = {
  TASK_STATE_SUBMITTED: "pending",
  TASK_STATE_WORKING: "running",
  TASK_STATE_INPUT_REQUIRED: "awaiting_input",
  TASK_STATE_COMPLETED: "completed",
  TASK_STATE_FAILED: "failed",
  TASK_STATE_CANCELED: "canceled",
};

function nodeIdFromArtifactId(artifactId: string): string | null {
  if (!artifactId || artifactId.startsWith("plan:")) return null;
  const index = artifactId.lastIndexOf(":");
  return index > 0 ? artifactId.slice(0, index) : null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function taskEventsFromSnapshot(
  task: ProtoStruct,
  baseSeq: number,
): EventDto[] {
  const events: EventDto[] = [];
  const push = (type: string, payload: ProtoStruct) => {
    events.push({ seq: baseSeq + events.length + 1, type, payload });
  };
  const state = String((task.status as ProtoStruct | undefined)?.state ?? "");
  const status = TASK_STATE_TO_STATUS[state];
  if (status) push("task.state_changed", { to: status });
  const nodes = (metadataOf(task).nodes as ProtoStruct[] | undefined) ?? [];
  for (const node of nodes) {
    push("node.state_changed", {
      node_id: node.id,
      to: node.status,
      agent_name: node.agent_name,
      attempt: node.attempt,
    });
  }
  const artifacts = (task.artifacts as ProtoStruct[] | undefined) ?? [];
  for (const artifact of artifacts) {
    const artifactId = String(artifact.artifactId ?? "");
    const nodeId = nodeIdFromArtifactId(artifactId);
    const text = textOfParts(artifact);
    if (nodeId && text) {
      push("node.artifact", { node_id: nodeId, artifact_id: artifactId, text, append: false });
    }
  }
  return events;
}

export function taskEventFromResult(
  result: ProtoStruct,
  seq: number,
): EventDto | null {
  const update = result.statusUpdate as ProtoStruct | undefined;
  if (update) {
    const metadata = metadataOf(update);
    if (typeof metadata.kind === "string") {
      return { seq, type: metadata.kind, payload: { ...metadata } };
    }
    const state = String((update.status as ProtoStruct | undefined)?.state ?? "");
    if (state === "TASK_STATE_COMPLETED") return { seq, type: "task.completed", payload: {} };
    if (state === "TASK_STATE_FAILED") return { seq, type: "task.failed", payload: {} };
    const status = TASK_STATE_TO_STATUS[state];
    return status ? { seq, type: "task.state_changed", payload: { to: status } } : null;
  }
  const artifactUpdate = result.artifactUpdate as ProtoStruct | undefined;
  if (!artifactUpdate) return null;
  const metadata = metadataOf(artifactUpdate);
  const artifact = (artifactUpdate.artifact as ProtoStruct | undefined) ?? {};
  const artifactId = String(artifact.artifactId ?? "");
  if (metadata.kind === "plan.created" || metadata.kind === "plan.extended") {
    const parts = (artifact.parts as ProtoStruct[] | undefined) ?? [];
    const dag = parts.length > 0 ? parts[0].data ?? null : null;
    return { seq, type: metadata.kind, payload: { ...metadata, dag } };
  }
  const nodeId =
    (typeof metadata.node_id === "string" && metadata.node_id) ||
    nodeIdFromArtifactId(artifactId);
  const text = textOfParts(artifact);
  if (!nodeId || !text) return null;
  return {
    seq,
    type: "node.artifact",
    payload: {
      node_id: nodeId,
      artifact_id: artifactId,
      text,
      append: artifactUpdate.append === true,
    },
  };
}

function runStream(
  request: ProtoStruct,
  handlers: SubscriptionHandlers,
  options: SubscriptionOptions,
  nextSeq: () => number,
  mapResponse: (result: ProtoStruct, seq: number) => void,
): () => void {
  const controller = new AbortController();
  let stopped = false;
  let attempt = 0;
  const run = async () => {
    while (!stopped) {
      try {
        let received = false;
        for await (const response of postSse(A2A_URL, request, controller.signal)) {
          if (response.error) {
            throw new A2AError(
              response.error.code ?? -32000,
              response.error.message ?? "A2A 订阅失败",
            );
          }
          if (!received) {
            received = true;
            attempt = 0;
            handlers.onState("live");
          }
          mapResponse(response.result ?? {}, nextSeq());
        }
        if (stopped) break;
        throw new A2AError(-32000, "A2A 流已断开");
      } catch {
        if (stopped || controller.signal.aborted) break;
        handlers.onState("reconnecting");
        await sleep((options.retryDelayMs ?? 1000) * Math.min(attempt + 1, 5));
        attempt += 1;
      }
    }
  };
  void run();
  return () => {
    stopped = true;
    controller.abort();
    handlers.onState("closed");
  };
}

export function subscribeRoom(
  conversationId: string,
  handlers: SubscriptionHandlers,
  options: SubscriptionOptions = {},
): () => void {
  let seq = options.baseSeq ?? 0;
  return runStream(
    rpcRequest("SubscribeToTask", { id: conversationId }),
    handlers,
    options,
    () => {
      seq += 1;
      return seq;
    },
    (result) => {
      if (result.task) {
        handlers.onSnapshot?.(result.task as ProtoStruct);
        return;
      }
      const event = roomEventFromResult(result);
      if (event) {
        if (event.seq > 0) seq = Math.max(seq, event.seq);
        handlers.onEvent(event);
      }
    },
  );
}

export function subscribeTask(
  taskId: string,
  handlers: SubscriptionHandlers,
  options: SubscriptionOptions = {},
): () => void {
  let seq = options.baseSeq ?? 0;
  return runStream(
    rpcRequest("SubscribeToTask", { id: taskId }),
    handlers,
    options,
    () => {
      seq += 1;
      return seq;
    },
    (result) => {
      if (result.task) {
        for (const event of taskEventsFromSnapshot(result.task as ProtoStruct, seq)) {
          seq = Math.max(seq, event.seq);
          handlers.onEvent(event);
        }
        return;
      }
      const event = taskEventFromResult(result, seq + 1);
      if (event) {
        seq = Math.max(seq, event.seq);
        handlers.onEvent(event);
      }
    },
  );
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `npm test -- src/api/a2a.test.ts`
Expected: PASS（23 tests）

- [ ] **Step 5: 提交**

```bash
git add frontend/src/api/a2a.ts frontend/src/api/a2a.test.ts
git commit -m "feat(web): 任务适配层与 A2A 订阅重连（M13）"
```

---

### Task 4: hooks 与 App 切换

**Files:**
- Modify: `frontend/src/hooks/useRoom.ts`
- Modify: `frontend/src/hooks/useConversation.ts`
- Modify: `frontend/src/App.tsx`
- Modify: `frontend/src/App.test.tsx`

- [ ] **Step 1: 切换 useRoom**

`frontend/src/hooks/useRoom.ts` 全文替换为：

```ts
import { useCallback, useEffect, useRef, useState } from "react";

import { sendRoomMessage, subscribeRoom } from "../api/a2a";
import { api } from "../api/client";
import {
  applyRoomEvent,
  emptyRoom,
  fromRoomSnapshot,
  mergeRoomSnapshot,
  type RoomView,
} from "../lib/roomView";
import type { RoomSendInput } from "../components/room/RoomComposer";

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : "请求失败";
}

export function useRoom(conversationId: string | null) {
  const [view, setView] = useState<RoomView>(emptyRoom);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const loadedRoom = useRef<string | null>(null);
  const viewRoom = useRef<string | null>(null);

  const load = useCallback(async () => {
    if (!conversationId) {
      loadedRoom.current = null;
      viewRoom.current = null;
      setView(emptyRoom);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const snapshot = await api.getRoomMessages(conversationId);
      const sameRoom =
        loadedRoom.current === conversationId ||
        viewRoom.current === conversationId;
      loadedRoom.current = conversationId;
      viewRoom.current = conversationId;
      setView((previous) =>
        sameRoom ? mergeRoomSnapshot(previous, snapshot) : fromRoomSnapshot(snapshot),
      );
    } catch (exc) {
      setError(messageOf(exc));
    } finally {
      setLoading(false);
    }
  }, [conversationId]);

  useEffect(() => {
    loadedRoom.current = null;
    viewRoom.current = null;
    setView(emptyRoom);
  }, [conversationId]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (!conversationId) return;
    return subscribeRoom(conversationId, {
      onSnapshot: (snapshot) => {
        const sameRoom = viewRoom.current === conversationId;
        loadedRoom.current = conversationId;
        viewRoom.current = conversationId;
        setView((previous) =>
          sameRoom ? mergeRoomSnapshot(previous, snapshot) : fromRoomSnapshot(snapshot),
        );
      },
      onEvent: (event) => setView((previous) => applyRoomEvent(previous, event)),
      onState: () => undefined,
    });
  }, [conversationId]);

  const send = useCallback(
    async (input: RoomSendInput) => {
      if (!conversationId) return;
      const posted = await sendRoomMessage(conversationId, input);
      viewRoom.current = conversationId;
      if (posted) {
        setView((previous) =>
          applyRoomEvent(previous, {
            seq: posted.seq,
            type: "message.posted",
            payload: {
              message_id: posted.message_id,
              conversation_id: conversationId,
              seq: posted.seq,
              role: "user",
              sender: "CEO",
              text: input.text,
              mentions: input.mentions ?? [],
              quote_id: input.quote_id ?? null,
              task_id: posted.task_id ?? null,
              created_at: new Date().toISOString(),
            },
          }),
        );
        return;
      }
      await load();
    },
    [conversationId, load],
  );

  return { view, loading, error, load, send };
}
```

- [ ] **Step 2: 切换 useConversation**

`frontend/src/hooks/useConversation.ts` 的 import 与订阅 effect 改为：

```ts
import { useCallback, useEffect, useRef, useState } from "react";

import { subscribeTask } from "../api/a2a";
import { api } from "../api/client";
import {
  applyEvent,
  fromSnapshot,
  isTerminal,
  mergeSnapshot,
  withConnection,
  type TaskView,
} from "../lib/taskView";
import type { TaskStatus } from "../lib/types";
```

（删除 `subscribeTaskEvents` import。）

订阅 effect 改为：

```ts
  useEffect(() => {
    for (const view of views) {
      const terminal = isTerminal(view.status);
      if (!terminal && !streams.current.has(view.id)) {
        const close = subscribeTask(
          view.id,
          {
            onEvent: (event) =>
              setViews((prev) =>
                prev.map((item) =>
                  item.id === view.id
                    ? applyEvent(item, {
                        ...event,
                        seq: Math.max(item.lastSeq + 1, event.seq),
                      })
                    : item,
                ),
              ),
            onState: (state) =>
              setViews((prev) =>
                prev.map((item) =>
                  item.id === view.id ? withConnection(item, state) : item,
                ),
              ),
          },
          { baseSeq: view.lastSeq },
        );
        streams.current.set(view.id, close);
      }
      if (terminal) {
        const close = streams.current.get(view.id);
        if (close) {
          close();
          streams.current.delete(view.id);
        }
        if (observed.current.get(view.id) !== view.status) {
          observed.current.set(view.id, view.status);
          void api
            .getTask(view.id)
            .then((snapshot) =>
              setViews((prev) =>
                prev.map((item) =>
                  item.id === view.id ? mergeSnapshot(item, snapshot) : item,
                ),
              ),
            )
            .catch(() => undefined);
        }
      } else {
        observed.current.set(view.id, view.status);
      }
    }
  }, [views]);
```

（其余 `load` / `appendTask` / `refreshTask` 不动。）

- [ ] **Step 3: 切换 App 首条消息发送**

`frontend/src/App.tsx`：
- import 增加：`import { sendRoomMessage } from "./api/a2a";`
- `send` 改为：

```ts
  const send = useCallback(
    async (text: string) => {
      setBanner(null);
      try {
        const created = await api.createConversation(text.slice(0, 40));
        navigate(created.conversation_id);
        await sendRoomMessage(created.conversation_id, { text });
        await refreshConversations();
      } catch (exc) {
        setBanner(messageOf(exc));
      }
    },
    [navigate, refreshConversations],
  );
```

- [ ] **Step 4: 更新 App 测试的 A2A mock**

`frontend/src/App.test.tsx` 全文替换为：

```tsx
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";
import { A2A_ROOM_URI } from "./api/a2a";

function roomMessage(overrides: Record<string, unknown>) {
  return {
    id: "m1",
    conversation_id: "c1",
    seq: 1,
    role: "user",
    sender: "CEO",
    text: "第一条",
    mentions: [],
    created_at: "2026-09-13T00:00:00+00:00",
    ...overrides,
  };
}

function sseStream(frames: string[]): Response {
  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const frame of frames) controller.enqueue(encoder.encode(frame));
    },
  });
  return new Response(body, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

function setupFetch(messages: unknown[]) {
  const calls: { url: string; method: string }[] = [];
  let posted = 0;
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = (init?.method ?? "GET").toUpperCase();
    calls.push({ url, method });
    const json = (data: unknown, status = 200) =>
      new Response(JSON.stringify(data), {
        status,
        headers: { "content-type": "application/json" },
      });

    if (url === "/v1/a2a" && method === "POST") {
      const rpc = JSON.parse(String(init?.body ?? "{}")) as {
        id: number;
        method: string;
        params?: { message?: { parts?: { text?: string }[] } };
      };
      if (rpc.method === "SubscribeToTask") return sseStream([]);
      if (rpc.method === "SendMessage") {
        posted += 1;
        const text = rpc.params?.message?.parts?.[0]?.text ?? "";
        const id = `m-new-${posted}`;
        const seq = 10 + posted;
        messages.push(roomMessage({ id, seq, text }));
        return json({
          jsonrpc: "2.0",
          id: rpc.id,
          result: {
            message: {
              messageId: id,
              contextId: "c1",
              taskId: "c1",
              role: "ROLE_USER",
              parts: [{ text }],
              metadata: {
                [A2A_ROOM_URI]: {
                  kind: "message",
                  sender: "CEO",
                  seq,
                  mentions: [],
                },
              },
            },
          },
        });
      }
    }
    if (url === "/v1/conversations" && method === "GET") return json([]);
    if (url === "/v1/conversations" && method === "POST") {
      return json({ conversation_id: "c1", title: "新对话" }, 201);
    }
    if (url === "/v1/conversations/c1" && method === "GET") {
      return json({
        conversation: {
          id: "c1",
          title: "新对话",
          created_at: "2026-09-13T00:00:00+00:00",
          updated_at: "2026-09-13T00:00:00+00:00",
          task_count: 0,
          last_status: "completed",
        },
        tasks: [],
      });
    }
    if (url.startsWith("/v1/conversations/c1/messages") && method === "GET") {
      return json({ messages, members: [], summary: null, last_seq: 1 });
    }
    return json({ detail: `unhandled ${method} ${url}` }, 500);
  });
  vi.stubGlobal("fetch", fetchMock);
  return { calls };
}

beforeEach(() => {
  window.history.pushState({}, "", "/");
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("App 群聊", () => {
  it("首条消息创建会话并展示 CEO 输入", async () => {
    const user = userEvent.setup();
    const { calls } = setupFetch([]);
    render(<App />);

    const composer = screen.getByPlaceholderText(
      "输入消息，Enter 发送，Shift+Enter 换行",
    );
    await user.type(composer, "第一条");
    await user.keyboard("{Enter}");

    expect(await screen.findByText("第一条")).toBeInTheDocument();
    expect(screen.getAllByText("CEO").length).toBeGreaterThan(0);
    expect(calls.some((call) => call.url === "/v1/a2a" && call.method === "POST")).toBe(true);
  });

  it("任务完成后仍可继续发言并立即显示", async () => {
    const user = userEvent.setup();
    window.history.pushState({}, "", "/?c=c1");
    setupFetch([
      roomMessage({ id: "m1", seq: 1, text: "第一条" }),
      roomMessage({
        id: "m2",
        seq: 2,
        role: "assistant",
        sender: "assistant",
        text: "任务完成：- echo：done",
      }),
    ]);
    render(<App />);

    const composer = await screen.findByPlaceholderText(
      "输入消息，@ 指派 Agent，Enter 发送",
    );
    await user.type(composer, "继续聊");
    await user.keyboard("{Enter}");

    expect(await screen.findByText("继续聊")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByText("任务完成：- echo：done")).toBeInTheDocument();
    });
  });
});
```

- [ ] **Step 5: 运行前端测试**

Run: `npm test`
Expected: 全部 PASS（既有 40 + 新增 23）

- [ ] **Step 6: 构建验证**

Run: `npm run build`
Expected: tsc 无错误、vite build 成功

- [ ] **Step 7: 提交**

```bash
git add frontend/src/hooks/useRoom.ts frontend/src/hooks/useConversation.ts \
  frontend/src/App.tsx frontend/src/App.test.tsx
git commit -m "feat(web): 房间与任务订阅发送切换到 A2A（M13）"
```

---

### Task 5: 文档与全量验证

**Files:**
- Modify: `docs/superpowers/specs/2026-09-13-a2a-facade-design.md`

- [ ] **Step 1: spec 补 M13 实施记录**

在 `docs/superpowers/specs/2026-09-13-a2a-facade-design.md` 末尾追加：

```markdown
## M13 实施记录

- 所有 A2A 请求带 `A2A-Version: 1.0` 头（实测缺省时服务端按 0.3 拒绝，`-32009`）；
- `postSse` 同时兼容：SSE 帧（`data:` 多行拼接、忽略注释、CRLF、跨 chunk）、非 SSE 的 JSON-RPC JSON 响应（版本/参数错误）；
- 任务事件适配：`statusUpdate.metadata.kind` 直接作为内部事件名（payload snake_case 原样透传）；无 kind 时按 `status.state` 映射终态；`artifactUpdate` 的 `plan.created/plan.extended` 取 parts[].data 作 dag，节点产物用 artifactId 前缀或 metadata.node_id，`lastChunk` 空帧忽略；
- 任务流无 event seq：适配层按订阅起点自增，hook 端以 `max(lastSeq+1, seq)` 兜底，REST 快照合并后不会丢帧；
- 断线重连由 `subscribeRoom`/`subscribeTask` 内部负责（退避 ×1..×5，默认 1s），首帧快照负责补缺口；旧 REST 端点与 `events.ts` 的 EventSource 实现保留但不再被 hooks 使用。
```

- [ ] **Step 2: 全量验证**

Run: `npm test && npm run build && npm run lint`
Expected: 全绿

Run: `uv run pytest -p no:warnings -q`（仓库根）
Expected: 249 passed（后端零改动）

- [ ] **Step 3: 提交**

```bash
git add docs/superpowers/specs/2026-09-13-a2a-facade-design.md
git commit -m "docs: M13 前端 A2A 迁移实施记录"
```

---

## Self-Review 记录

- **Spec 覆盖**：§8 `postSse`/`sendMessage`/`getTask`/`subscribeTask` → Task 1–3（`getTask` 未用：阶段 1 快照仍走 REST，spec 允许；`subscribeTask` 即本计划 `subscribeTask`+`subscribeRoom`）；适配层四类映射 → Task 2/3；hooks 切换 → Task 4；断线重连 → Task 3 `runStream`；旧端点保留 → Task 4 未删 `events.ts`。§9 M13 验收（40 测试 + A2A 回归）→ Task 4/5。§10 前端测试 → Task 1–3 夹具单测。
- **占位符扫描**：无 TBD；所有步骤含完整代码与期望输出。
- **类型一致性**：`ProtoStruct`、`metadataOf`/`roomMetaOf`、`roomSnapshotFromTask`、`roomEventFromResult`、`taskEventsFromSnapshot`、`taskEventFromResult`、`subscribeRoom`/`subscribeTask`、`sendRoomMessage`、`sendMessageParams` 在任务间命名一致；`EventDto`/`RoomMessagesDto`/`PostMessageOutDto` 复用 `lib/types.ts` 既有类型。
- **已知取舍**：任务流 seq 为前端合成值（hook 端兜底到 `lastSeq+1`），仅用于 view-model 去重；`taskEventsFromSnapshot` 不还原 plan（REST 快照已含）；`delivered_at` 时间戳用本地时间。
