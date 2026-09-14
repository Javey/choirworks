import { render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";

beforeEach(() => {
  window.history.pushState({}, "", "/");
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function setupFetch() {
  const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = (init?.method ?? "GET").toUpperCase();
    const json = (data: unknown, status = 200) =>
      new Response(JSON.stringify(data), {
        status,
        headers: { "content-type": "application/json" },
      });

    if (url === "/v1/conversations" && method === "GET") return json([]);
    if (url === "/v1/a2a" && method === "POST") {
      const body = JSON.parse(String(init?.body ?? "{}"));
      if (body.method === "SendMessage") {
        const text = body.params?.message?.parts?.[0]?.text ?? "hello";
        return json({
          jsonrpc: "2.0",
          id: body.id,
          result: {
            task: {
              id: "task-1",
              contextId: "task-1",
              status: { state: "TASK_STATE_COMPLETED" },
              history: [
                {
                  messageId: "m1",
                  role: "ROLE_USER",
                  parts: [{ text }],
                  metadata: {
                    "https://github.com/Javey/choirworks/extensions/room/v1": {
                      sender: "CEO",
                      role: "user",
                    },
                  },
                },
              ],
            },
          },
        });
      }
      if (body.method === "GetTask") {
        return json({
          jsonrpc: "2.0",
          id: body.id,
          result: {
            task: {
              id: body.params?.id ?? "task-1",
              contextId: body.params?.id ?? "task-1",
              status: { state: "TASK_STATE_COMPLETED" },
              history: [
                {
                  messageId: "m1",
                  role: "ROLE_USER",
                  parts: [{ text: "hello" }],
                  metadata: {
                    "https://github.com/Javey/choirworks/extensions/room/v1": {
                      sender: "CEO",
                      role: "user",
                    },
                  },
                },
              ],
            },
          },
        });
      }
    }
    return json({ detail: `unhandled ${method} ${url}` }, 500);
  });
  vi.stubGlobal("fetch", fetchMock);
}

describe("App", () => {
  it("renders the initial empty state", async () => {
    setupFetch();
    render(<App />);
    expect(screen.getByText("开始协作")).toBeInTheDocument();
  });

  it("shows the composer input on initial state", async () => {
    setupFetch();
    render(<App />);
    expect(
      screen.getByPlaceholderText("输入消息，Enter 发送，Shift+Enter 换行"),
    ).toBeInTheDocument();
  });
});
