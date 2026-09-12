import type {
  CheckpointDto,
  ConversationDetailDto,
  ConversationSummaryDto,
  CreateTaskOutDto,
  RollbackReportDto,
  TaskSnapshotDto,
} from "../lib/types";

export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const response = await fetch(path, {
    method,
    headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok) {
    const detail =
      data && typeof data === "object" && "detail" in data
        ? String((data as { detail: unknown }).detail)
        : `请求失败（HTTP ${response.status}）`;
    throw new ApiError(response.status, detail);
  }
  return data as T;
}

export const api = {
  listConversations: () => request<ConversationSummaryDto[]>("GET", "/v1/conversations"),
  getConversation: (conversationId: string) =>
    request<ConversationDetailDto>("GET", `/v1/conversations/${conversationId}`),
  createTask: (text: string, conversationId?: string | null) =>
    request<CreateTaskOutDto>("POST", "/v1/tasks", {
      request: text,
      ...(conversationId ? { conversation_id: conversationId } : {}),
    }),
  getTask: (taskId: string) => request<TaskSnapshotDto>("GET", `/v1/tasks/${taskId}`),
  listCheckpoints: (taskId: string) =>
    request<CheckpointDto[]>("GET", `/v1/tasks/${taskId}/checkpoints`),
  answerIntervention: (taskId: string, interventionId: string, text: string) =>
    request<unknown>(
      "POST",
      `/v1/tasks/${taskId}/interventions/${interventionId}`,
      { text },
    ),
  rollback: (taskId: string, checkpointId: string, mode: "restart" | "dry_run") =>
    request<RollbackReportDto>("POST", `/v1/tasks/${taskId}/rollback`, {
      checkpoint_id: checkpointId,
      mode,
    }),
  retryNode: (taskId: string, nodeId: string) =>
    request<TaskSnapshotDto>("POST", `/v1/tasks/${taskId}/nodes/${nodeId}/retry`),
  cancelTask: (taskId: string) =>
    request<TaskSnapshotDto>("POST", `/v1/tasks/${taskId}/cancel`),
};
