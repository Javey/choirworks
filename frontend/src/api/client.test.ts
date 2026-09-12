import { afterEach, describe, expect, it, vi } from "vitest";

import { ApiError, api } from "./client";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("api client", () => {
  it("creates a task and passes conversation_id", async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({ task_id: "t1", conversation_id: "c1" }, 201),
    );
    vi.stubGlobal("fetch", fetchMock);

    const created = await api.createTask("你好", "c1");
    expect(created.task_id).toBe("t1");
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/v1/tasks");
    expect(init.method).toBe("POST");
    expect(JSON.parse(String(init.body))).toEqual({
      request: "你好",
      conversation_id: "c1",
    });
  });

  it("omits conversation_id for a new conversation", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({ task_id: "t1" }, 201));
    vi.stubGlobal("fetch", fetchMock);

    await api.createTask("你好");
    const [, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(JSON.parse(String(init.body))).toEqual({ request: "你好" });
  });

  it("throws ApiError with server detail", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => jsonResponse({ detail: "conversation not found: c9" }, 404)),
    );
    const error = await api.getConversation("c9").catch((exc: unknown) => exc);
    expect(error).toBeInstanceOf(ApiError);
    expect((error as ApiError).status).toBe(404);
    expect((error as ApiError).message).toBe("conversation not found: c9");
  });

  it("calls rollback with mode", async () => {
    const fetchMock = vi.fn(async () =>
      jsonResponse({ checkpoint_id: "ck", mode: "dry_run" }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await api.rollback("t1", "ck", "dry_run");
    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("/v1/tasks/t1/rollback");
    expect(JSON.parse(String(init.body))).toEqual({
      checkpoint_id: "ck",
      mode: "dry_run",
    });
  });
});
