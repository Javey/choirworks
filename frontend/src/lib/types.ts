export type TaskStatus =
  | "pending"
  | "planning"
  | "running"
  | "awaiting_input"
  | "completed"
  | "failed"
  | "canceled";

export type NodeStatus =
  | "pending"
  | "ready"
  | "dispatched"
  | "working"
  | "input_required"
  | "completed"
  | "failed"
  | "canceled"
  | "invalidated";

export type InterventionStatus = "pending" | "resolved" | "expired" | "failed" | "invalidated";

export interface PlanNodeDto {
  id: string;
  name: string;
  agent_name?: string;
  deps: string[];
  input?: Record<string, unknown>;
  derived?: boolean;
}

export interface PlanDto {
  id: string;
  task_id: string;
  version: number;
  rationale?: string | null;
  dag: { nodes: PlanNodeDto[] };
  created_at: string;
}

export interface NodeDto {
  id: string;
  task_id: string;
  plan_id: string;
  name: string;
  agent_name?: string | null;
  status: NodeStatus;
  attempt: number;
  input?: Record<string, unknown> | null;
  output?: { artifacts?: { text?: string }[] } | null;
  error?: string | null;
}

export interface TaskDto {
  id: string;
  status: TaskStatus;
  request: string;
  conversation_id?: string | null;
  plan_version?: number | null;
  created_at: string;
  updated_at: string;
}

export interface TaskSnapshotDto {
  task: TaskDto;
  plan: PlanDto | null;
  nodes: NodeDto[];
  last_seq: number;
}

export interface ConversationSummaryDto {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
  task_count: number;
  last_status: TaskStatus;
}

export interface ConversationDetailDto {
  id: string;
  tasks: Record<string, unknown>[];
}

export interface CreateTaskOutDto {
  task_id: string;
  plan_id?: string | null;
  node_ids?: string[];
  conversation_id?: string | null;
}

export interface InterventionDto {
  id: string;
  task_id: string;
  node_id?: string | null;
  assigned_node_id?: string | null;
  assigned_to?: string | null;
  source: string;
  policy: string;
  question: { text?: string } & Record<string, unknown>;
  answer?: { text?: string } | null;
  responder?: string | null;
  status: InterventionStatus;
  deadline_at?: string | null;
  created_at: string;
  resolved_at?: string | null;
}

export interface CheckpointDto {
  id: string;
  task_id: string;
  seq: number;
  plan_version: number;
  frontier: string[];
  artifacts: Record<string, unknown>;
  created_at: string;
}

export interface RollbackReportDto {
  checkpoint_id: string;
  plan_version: number;
  reset_node_ids: string[];
  cancelled_remote_task_ids: string[];
  mode: string;
}

export interface EventDto {
  seq: number;
  type: string;
  payload: Record<string, unknown>;
}

export type RoomRole = "user" | "assistant" | "agent" | "system";

export interface RoomMessageDto {
  id: string;
  conversation_id: string;
  seq: number;
  role: RoomRole;
  sender?: string | null;
  text: string;
  mentions: string[];
  quote_id?: string | null;
  task_id?: string | null;
  node_id?: string | null;
  intervention_id?: string | null;
  queued_for_node_id?: string | null;
  delivered_at?: string | null;
  created_at: string;
}

export interface RoomMemberDto {
  conversation_id: string;
  agent_name: string;
  agent_url: string;
  reason?: string | null;
  joined_at: string;
}

export interface RoomSummaryDto {
  conversation_id: string;
  covers_seq: number;
  summary: Record<string, unknown>;
  updated_at: string;
}

export interface RoomMessagesDto {
  messages: RoomMessageDto[];
  members: RoomMemberDto[];
  summary: RoomSummaryDto | null;
  last_seq: number;
}

export interface PostMessageOutDto {
  message_id: string;
  seq: number;
  task_id?: string | null;
}
