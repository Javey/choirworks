import type {
  EventDto,
  PostMessageOutDto,
  RoomMessageDto,
  RoomMessagesDto,
  TaskStatus,
} from "../lib/types";

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
      payload: {
        message_id: message.id,
        conversation_id: message.conversation_id,
        seq: message.seq,
        role: message.role,
        sender: message.sender,
        text: message.text,
        mentions: message.mentions,
        quote_id: message.quote_id,
        task_id: message.task_id,
        node_id: message.node_id,
        intervention_id: message.intervention_id,
        queued_for_node_id: message.queued_for_node_id,
        created_at: message.created_at,
      },
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
      payload: {
        message_id: room.message_id ?? null,
        node_id: room.node_id ?? null,
      },
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
  if (input.mentions && input.mentions.length > 0) {
    roomMeta.mentions = input.mentions;
  }
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

export type ConnectionState = "live" | "reconnecting" | "closed";

export interface SubscriptionHandlers {
  onEvent: (event: EventDto) => void;
  onState: (state: ConnectionState) => void;
}

export interface RoomSubscriptionHandlers extends SubscriptionHandlers {
  onSnapshot?: (snapshot: RoomMessagesDto) => void;
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
      push("node.artifact", {
        node_id: nodeId,
        artifact_id: artifactId,
        text,
        append: false,
      });
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
    if (state === "TASK_STATE_COMPLETED") {
      return { seq, type: "task.completed", payload: {} };
    }
    if (state === "TASK_STATE_FAILED") {
      return { seq, type: "task.failed", payload: {} };
    }
    const status = TASK_STATE_TO_STATUS[state];
    return status
      ? { seq, type: "task.state_changed", payload: { to: status } }
      : null;
  }
  const artifactUpdate = result.artifactUpdate as ProtoStruct | undefined;
  if (!artifactUpdate) return null;
  const metadata = metadataOf(artifactUpdate);
  const artifact = (artifactUpdate.artifact as ProtoStruct | undefined) ?? {};
  const artifactId = String(artifact.artifactId ?? "");
  if (metadata.kind === "plan.created" || metadata.kind === "plan.extended") {
    const parts = (artifact.parts as ProtoStruct[] | undefined) ?? [];
    const dag = parts.length > 0 ? (parts[0].data ?? null) : null;
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
        for await (const response of postSse(
          A2A_URL,
          request,
          controller.signal,
        )) {
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
  handlers: RoomSubscriptionHandlers,
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
        handlers.onSnapshot?.(roomSnapshotFromTask(result.task as ProtoStruct));
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
    (result, eventSeq) => {
      if (result.task) {
        for (const event of taskEventsFromSnapshot(
          result.task as ProtoStruct,
          seq,
        )) {
          seq = Math.max(seq, event.seq);
          handlers.onEvent(event);
        }
        return;
      }
      const event = taskEventFromResult(result, eventSeq);
      if (event) {
        seq = Math.max(seq, event.seq);
        handlers.onEvent(event);
      }
    },
  );
}
