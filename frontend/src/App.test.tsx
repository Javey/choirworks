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
    expect(
      calls.some((call) => call.url === "/v1/a2a" && call.method === "POST"),
    ).toBe(true);
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
