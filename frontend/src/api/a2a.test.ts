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
