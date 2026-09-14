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

    // Agent card
    if (url.endsWith("/.well-known/agent-card.json") && method === "GET") {
      return json({
        name: "ChoirWorks",
        version: "0.1.0",
        supportedInterfaces: [
          { protocolBinding: "HTTP+JSON", url: "http://localhost/v1", protocolVersion: "1.0" },
        ],
      });
    }

    // REST: POST /v1/message:send
    if (url.endsWith("/v1/message:send") && method === "POST") {
      return json({
        id: "task-1",
        contextId: "task-1",
        status: { state: 3 },
        history: [
          {
            messageId: "m1",
            role: 1,
            parts: [{ content: { $case: "text", value: "hello" } }],
            metadata: {
              sender: "CEO",
            },
          },
        ],
        artifacts: [],
      });
    }

    // REST: GET /v1/tasks/{id}
    if (url.includes("/v1/tasks/") && method === "GET") {
      const id = url.split("/v1/tasks/")[1].split("?")[0];
      return json({
        id,
        contextId: id,
        status: { state: 3 },
        history: [
          {
            messageId: "m1",
            role: 1,
            parts: [{ content: { $case: "text", value: "hello" } }],
            metadata: {
              sender: "CEO",
            },
          },
        ],
        artifacts: [],
      });
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
