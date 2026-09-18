import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App from "./App";
import { getClient } from "./api/a2a-client";

vi.mock("./api/a2a-client", () => ({ getClient: vi.fn() }));

beforeEach(() => {
  window.history.pushState({}, "", "/");
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.clearAllMocks();
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

    if (url.endsWith("/.well-known/agent-card.json") && method === "GET") {
      return json({
        name: "ChoirWorks",
        version: "0.1.0",
        supportedInterfaces: [
          { protocolBinding: "HTTP+JSON", url: "http://localhost/v1", protocolVersion: "1.0" },
        ],
      });
    }

    if (url.endsWith("/v1/message:send") && method === "POST") {
      return json({
        id: "task-1",
        contextId: "ctx-1",
        status: { state: 3 },
        history: [],
        artifacts: [],
      });
    }

    if (url.includes("/v1/conversations/") && method === "GET") {
      return json({ id: "ctx-1", tasks: [] });
    }

    return json({ detail: `unhandled ${method} ${url}` }, 500);
  });
  vi.stubGlobal("fetch", fetchMock);
}

describe("App", () => {
  it("renders the header and empty event list", async () => {
    setupFetch();
    render(<App />);
    expect(screen.getByText("等待事件…")).toBeInTheDocument();
  });

  it("renders the composer input", async () => {
    setupFetch();
    render(<App />);
    expect(
      screen.getByPlaceholderText("输入消息，@ 指派 Agent，Enter 发送"),
    ).toBeInTheDocument();
  });

  it("navigates to the new conversation before the stream completes", async () => {
    setupFetch();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const client = {
      sendMessageStream: () =>
        (async function* () {
          yield {
            payload: {
              $case: "task",
              value: {
                id: "task-1",
                contextId: "ctx-1",
                status: { state: 3 },
                history: [],
                artifacts: [],
              },
            },
          };
          await gate;
        })(),
    } as unknown as Awaited<ReturnType<typeof getClient>>;
    vi.mocked(getClient).mockResolvedValue(client);

    render(<App />);
    const composer = screen.getByPlaceholderText(
      "输入消息，@ 指派 Agent，Enter 发送",
    );
    fireEvent.change(composer, { target: { value: "分析 X" } });
    fireEvent.click(screen.getByRole("button", { name: /发送/ }));

    await waitFor(() => expect(window.location.search).toBe("?c=ctx-1"));
    expect(screen.getByRole("button", { name: /发送/ })).toBeDisabled();

    release();
    await waitFor(() => expect(composer).toHaveValue(""));
  });
});
