import { Role, TaskState, roleToJSON, taskStateToJSON } from "@a2a-js/sdk";

type Rec = { [key: string]: unknown };

function isRec(value: unknown): value is Rec {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const warn = (label: string, key: string, value: unknown) =>
  console.warn(`[coreToWire] ${label}: unexpected field "${key}"`, value);

// --------------------------------------------------------------- Part

function corePartToWire(part: unknown): Rec {
  if (!isRec(part)) return {};
  const content = part.content as { $case?: string; value?: unknown } | undefined;

  const result: Rec = {};

  if (content?.$case === "text") {
    result.text = content.value;
  } else if (content?.$case === "raw") {
    const v = content.value;
    let b64: string;
    if (typeof v === "string") {
      b64 = v;
    } else if (v instanceof Uint8Array) {
      b64 = btoa(String.fromCharCode(...v));
    } else if (Array.isArray(v)) {
      b64 = btoa(String.fromCharCode(...v));
    } else {
      b64 = String(v);
    }
    result.file = { fileWithBytes: b64 };
  } else if (content?.$case === "url") {
    result.file = { fileWithUri: content.value };
  } else if (content?.$case === "data") {
    result.data = { data: content.value };
  } else if (content?.$case) {
    console.warn(`[coreToWire] Part: unknown content.$case "${content.$case}"`, content.value);
  } else {
    console.warn("[coreToWire] Part: missing content", part);
  }

  if (part.metadata) result.metadata = part.metadata;
  if (part.mediaType) result.mediaType = part.mediaType;
  if (part.filename) result.filename = part.filename;

  for (const key of Object.keys(part)) {
    if (!["content", "metadata", "mediaType", "filename"].includes(key)) {
      warn("Part", key, part[key]);
    }
  }

  return result;
}

// --------------------------------------------------------------- Message

function coreMessageToWire(msg: unknown): Rec {
  if (!isRec(msg)) return {};
  const result: Rec = {
    messageId: msg.messageId ?? "",
    role: roleToJSON(msg.role as Role),
    parts: Array.isArray(msg.parts) ? msg.parts.map(corePartToWire) : [],
  };
  if (msg.contextId) result.contextId = msg.contextId;
  if (msg.taskId) result.taskId = msg.taskId;
  if (msg.metadata) result.metadata = msg.metadata;
  if (Array.isArray(msg.extensions) && msg.extensions.length > 0)
    result.extensions = msg.extensions;
  if (Array.isArray(msg.referenceTaskIds) && msg.referenceTaskIds.length > 0)
    result.referenceTaskIds = msg.referenceTaskIds;

  for (const key of Object.keys(msg)) {
    if (
      ![
        "messageId",
        "role",
        "parts",
        "contextId",
        "taskId",
        "metadata",
        "extensions",
        "referenceTaskIds",
      ].includes(key)
    ) {
      warn("Message", key, msg[key]);
    }
  }

  return result;
}

// --------------------------------------------------------------- Artifact

function coreArtifactToWire(art: unknown): Rec {
  if (!isRec(art)) return {};
  const result: Rec = {
    artifactId: art.artifactId ?? "",
    parts: Array.isArray(art.parts) ? art.parts.map(corePartToWire) : [],
  };
  if (art.name) result.name = art.name;
  if (art.description) result.description = art.description;
  if (art.metadata) result.metadata = art.metadata;
  if (Array.isArray(art.extensions) && art.extensions.length > 0)
    result.extensions = art.extensions;

  for (const key of Object.keys(art)) {
    if (
      !["artifactId", "name", "description", "parts", "metadata", "extensions"].includes(
        key,
      )
    ) {
      warn("Artifact", key, art[key]);
    }
  }

  return result;
}

// --------------------------------------------------------------- TaskStatus

function coreTaskStatusToWire(status: unknown): Rec {
  if (!isRec(status)) return {};
  const result: Rec = {
    state: taskStateToJSON(status.state as TaskState),
  };
  if (status.timestamp) result.timestamp = status.timestamp;
  if (status.message) result.message = coreMessageToWire(status.message);

  for (const key of Object.keys(status)) {
    if (!["state", "timestamp", "message"].includes(key)) {
      warn("TaskStatus", key, status[key]);
    }
  }

  return result;
}

// --------------------------------------------------------------- Task

function coreTaskToWire(task: unknown): Rec {
  if (!isRec(task)) return {};
  const result: Rec = {
    id: task.id ?? "",
    contextId: task.contextId ?? "",
    status: coreTaskStatusToWire(task.status),
  };
  if (Array.isArray(task.artifacts) && task.artifacts.length > 0)
    result.artifacts = task.artifacts.map(coreArtifactToWire);
  if (Array.isArray(task.history) && task.history.length > 0)
    result.history = task.history.map(coreMessageToWire);
  if (task.metadata) result.metadata = task.metadata;

  for (const key of Object.keys(task)) {
    if (!["id", "contextId", "status", "artifacts", "history", "metadata"].includes(key)) {
      warn("Task", key, task[key]);
    }
  }

  return result;
}

// --------------------------------------------------------------- StatusUpdate

function coreStatusUpdateToWire(su: unknown): Rec {
  if (!isRec(su)) return {};
  const result: Rec = {
    taskId: su.taskId ?? "",
    contextId: su.contextId ?? "",
    status: coreTaskStatusToWire(su.status),
  };
  if (su.metadata) result.metadata = su.metadata;

  for (const key of Object.keys(su)) {
    if (!["taskId", "contextId", "status", "metadata", "final"].includes(key)) {
      warn("StatusUpdate", key, su[key]);
    }
  }

  // 'final' field exists in wire/proto format but is dropped by SDK toCore conversion
  if (su.final !== undefined) {
    console.warn(
      "[coreToWire] StatusUpdate: 'final' field found in core object (SDK should have dropped it)",
      su.final,
    );
    result.final = su.final;
  } else {
    console.warn(
      "[coreToWire] StatusUpdate: 'final' field not recoverable (dropped by SDK toCoreStreamResponse)",
    );
  }

  return result;
}

// --------------------------------------------------------------- ArtifactUpdate

function coreArtifactUpdateToWire(au: unknown): Rec {
  if (!isRec(au)) return {};
  const result: Rec = {
    taskId: au.taskId ?? "",
    contextId: au.contextId ?? "",
    artifact: coreArtifactToWire(au.artifact),
  };
  if (au.append === true) result.append = true;
  if (au.lastChunk === true) result.lastChunk = true;
  if (au.metadata) result.metadata = au.metadata;

  for (const key of Object.keys(au)) {
    if (
      !["taskId", "contextId", "artifact", "append", "lastChunk", "metadata"].includes(key)
    ) {
      warn("ArtifactUpdate", key, au[key]);
    }
  }

  return result;
}

// --------------------------------------------------------------- StreamResponse

export function coreToWire(event: unknown): unknown {
  if (!isRec(event)) return event;
  const payload = event.payload as { $case?: string; value?: unknown } | undefined;
  if (!payload?.$case || payload.value === undefined) return event;

  for (const key of Object.keys(event)) {
    if (key !== "payload") {
      warn("StreamResponse", key, event[key]);
    }
  }

  switch (payload.$case) {
    case "task":
      return { task: coreTaskToWire(payload.value) };
    case "statusUpdate":
      return { statusUpdate: coreStatusUpdateToWire(payload.value) };
    case "artifactUpdate":
      return { artifactUpdate: coreArtifactUpdateToWire(payload.value) };
    case "message":
      return { message: coreMessageToWire(payload.value) };
    default:
      console.warn(
        `[coreToWire] StreamResponse: unknown payload.$case "${payload.$case}"`,
      );
      return event;
  }
}
