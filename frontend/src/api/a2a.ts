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
