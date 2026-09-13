import type {
  EventDto,
  InterventionDto,
  NodeDto,
  NodeStatus,
  TaskSnapshotDto,
  TaskStatus,
} from "./types";

export interface NodeView {
  id: string;
  name: string;
  agentName?: string;
  status: NodeStatus;
  attempt: number;
  inputText?: string;
  outputText?: string;
  error?: string;
  order: number;
  superseded?: boolean;
}

export interface InterventionView {
  id: string;
  nodeId?: string;
  status: InterventionDto["status"];
  policy: string;
  source: string;
  questionText: string;
  answerText?: string;
  responder?: string;
  deadlineAt?: string;
}

export interface NoteView {
  id: string;
  level: "info" | "warn";
  text: string;
}

export interface TaskView {
  id: string;
  request: string;
  status: TaskStatus;
  conversationId?: string | null;
  rationale?: string;
  nodes: NodeView[];
  nodeOrder: string[];
  interventions: InterventionView[];
  notes: NoteView[];
  connection: "live" | "reconnecting" | "closed";
  lastSeq: number;
  createdAt: string;
  updatedAt: string;
}

const TERMINAL_TASK_STATUSES: TaskStatus[] = ["completed", "failed", "canceled"];

export function isTerminal(status: TaskStatus): boolean {
  return TERMINAL_TASK_STATUSES.includes(status);
}

function artifactText(output: NodeDto["output"]): string | undefined {
  const parts = (output?.artifacts ?? [])
    .map((artifact) => artifact.text ?? "")
    .filter((text) => text.length > 0);
  return parts.length > 0 ? parts.join(" ") : undefined;
}

function textOf(value: unknown): string | undefined {
  if (typeof value === "string") return value;
  if (value && typeof value === "object" && "text" in value) {
    const text = (value as { text?: unknown }).text;
    if (typeof text === "string") return text;
  }
  return undefined;
}

function nodeView(node: NodeDto, order: number): NodeView {
  return {
    id: node.id,
    name: node.name,
    agentName: node.agent_name ?? undefined,
    status: node.status,
    attempt: node.attempt,
    inputText: textOf(node.input),
    outputText: artifactText(node.output),
    error: node.error ?? undefined,
    order,
  };
}

export function fromSnapshot(snapshot: TaskSnapshotDto): TaskView {
  const planOrder = (snapshot.plan?.dag.nodes ?? []).map(
    (node) => `${snapshot.plan?.id}:${node.id}`,
  );
  const orders = new Map(planOrder.map((id, index) => [id, index]));
  const nodes = snapshot.nodes
    .map((node) => nodeView(node, orders.get(node.id) ?? orders.size + 1))
    .sort((left, right) => left.order - right.order);
  return {
    id: snapshot.task.id,
    request: snapshot.task.request,
    status: snapshot.task.status,
    conversationId: snapshot.task.conversation_id,
    rationale: snapshot.plan?.rationale ?? undefined,
    nodes,
    nodeOrder: nodes.map((node) => node.id),
    interventions: [],
    notes: [],
    connection: "live",
    lastSeq: snapshot.last_seq,
    createdAt: snapshot.task.created_at,
    updatedAt: snapshot.task.updated_at,
  };
}

export function pendingTaskView(
  taskId: string,
  request: string,
  conversationId?: string | null,
): TaskView {
  const now = new Date().toISOString();
  return {
    id: taskId,
    request,
    status: "pending",
    conversationId,
    nodes: [],
    nodeOrder: [],
    interventions: [],
    notes: [],
    connection: "live",
    lastSeq: 0,
    createdAt: now,
    updatedAt: now,
  };
}

function withNode(view: TaskView, nodeId: string, update: (node: NodeView) => NodeView): TaskView {
  const existing = view.nodes.find((node) => node.id === nodeId);
  const nodes = existing
    ? view.nodes.map((node) => (node.id === nodeId ? update(node) : node))
    : [
        ...view.nodes,
        update({
          id: nodeId,
          name: nodeId,
          status: "pending",
          attempt: 0,
          order: view.nodes.length,
        }),
      ];
  const nodeOrder = view.nodeOrder.includes(nodeId)
    ? view.nodeOrder
    : [...view.nodeOrder, nodeId];
  return { ...view, nodes, nodeOrder };
}

function addNote(view: TaskView, event: EventDto, text: string, level: "info" | "warn" = "info") {
  return {
    ...view,
    notes: [...view.notes, { id: `${event.seq}`, level, text }],
  };
}

export function applyEvent(view: TaskView, event: EventDto): TaskView {
  if (event.seq <= view.lastSeq) return view;
  const payload = event.payload;
  const nodeId = typeof payload.node_id === "string" ? payload.node_id : undefined;
  const base: TaskView = { ...view, lastSeq: event.seq };
  const now = new Date().toISOString();

  switch (event.type) {
    case "task.created":
      return {
        ...base,
        request: typeof payload.request === "string" ? payload.request : view.request,
        conversationId:
          typeof payload.conversation_id === "string"
            ? payload.conversation_id
            : view.conversationId,
        updatedAt: now,
      };
    case "plan.created": {
      const dag = payload.dag as { nodes?: { id: string; name: string; agent_name?: string }[] };
      const planId = String(payload.plan_id ?? "");
      const nextIds = (dag?.nodes ?? []).map((node) => `${planId}:${node.id}`);
      const superseded =
        nextIds.length > 0
          ? view.nodes.map((node) =>
              nextIds.includes(node.id) ? { ...node, superseded: false } : { ...node, superseded: true },
            )
          : view.nodes;
      let next: TaskView = {
        ...base,
        nodes: superseded,
        rationale: typeof payload.rationale === "string" ? payload.rationale : view.rationale,
      };
      next = addNote(
        next,
        event,
        `已生成计划 v${String(payload.version ?? "")}（${nextIds.length} 个节点）`,
      );
      (dag?.nodes ?? []).forEach((dagNode, index) => {
        next = withNode(next, `${planId}:${dagNode.id}`, (node) => ({
          ...node,
          name: dagNode.name,
          agentName: dagNode.agent_name,
          order: view.nodes.length + index,
        }));
      });
      return { ...next, updatedAt: now };
    }
    case "plan.superseded":
      return base;
    case "node.dispatch.intent":
      if (!nodeId) return base;
      return withNode(base, nodeId, (node) => ({
        ...node,
        attempt: typeof payload.attempt === "number" ? payload.attempt : node.attempt,
      }));
    case "node.dispatched":
      if (!nodeId) return base;
      return withNode(base, nodeId, (node) => ({ ...node, status: "dispatched" }));
    case "node.state_changed": {
      if (!nodeId) return base;
      const to = payload.to as NodeStatus;
      return withNode(base, nodeId, (node) => ({ ...node, status: to }));
    }
    case "node.artifact": {
      if (!nodeId) return base;
      const piece = typeof payload.text === "string" ? payload.text : "";
      const append = payload.append === true;
      return withNode(base, nodeId, (node) => ({
        ...node,
        outputText: append && node.outputText ? node.outputText + piece : piece,
      }));
    }
    case "node.output": {
      if (!nodeId) return base;
      const output = payload.output as NodeDto["output"];
      return withNode(base, nodeId, (node) => ({ ...node, outputText: artifactText(output) }));
    }
    case "node.retry.scheduled":
      if (!nodeId) return base;
      return addNote(
        withNode(base, nodeId, (node) => ({
          ...node,
          attempt: typeof payload.attempt === "number" ? payload.attempt : node.attempt,
        })),
        event,
        `第 ${String(payload.attempt ?? "")} 次重试将在 ${String(payload.delay_seconds ?? 0)}s 后开始`,
        "warn",
      );
    case "node.invalidated":
      if (!nodeId) return base;
      return addNote(
        withNode(base, nodeId, (node) => ({ ...node, status: "invalidated" })),
        event,
        "节点已失效",
        "warn",
      );
    case "node.cancel.sent":
      return addNote(base, event, "已向远程 Agent 发送取消信号", "warn");
    case "intervention.requested": {
      const intervention: InterventionView = {
        id: String(payload.intervention_id),
        nodeId,
        status: "pending",
        policy: String(payload.policy ?? ""),
        source: String(payload.source ?? ""),
        questionText: textOf(payload.question) ?? "Agent 需要补充信息",
        deadlineAt: typeof payload.deadline_at === "string" ? payload.deadline_at : undefined,
      };
      return {
        ...base,
        interventions: [...view.interventions, intervention],
        updatedAt: now,
      };
    }
    case "intervention.resolved": {
      const interventionId = String(payload.intervention_id);
      return {
        ...base,
        interventions: view.interventions.map((item) =>
          item.id === interventionId
            ? {
                ...item,
                status: "resolved",
                answerText: textOf(payload.answer),
                responder:
                  typeof payload.responder === "string" ? payload.responder : item.responder,
              }
            : item,
        ),
      };
    }
    case "checkpoint.created":
      return base;
    case "rollback.performed": {
      const resetIds = (payload.reset_node_ids as string[] | undefined) ?? [];
      let next: TaskView = base;
      resetIds.forEach((id) => {
        next = withNode(next, id, (node) => ({
          ...node,
          status: "pending",
          attempt: 0,
          outputText: undefined,
          error: undefined,
        }));
      });
      return addNote(
        next,
        event,
        `已回退到 checkpoint ${String(payload.checkpoint_id ?? "")}，重置 ${resetIds.length} 个节点`,
        "warn",
      );
    }
    case "error": {
      const message = typeof payload.message === "string" ? payload.message : "未知错误";
      const next = nodeId
        ? withNode(base, nodeId, (node) => ({ ...node, error: message }))
        : base;
      return addNote(next, event, message, "warn");
    }
    case "task.state_changed":
      return { ...base, status: payload.to as TaskStatus, updatedAt: now };
    case "task.completed":
      return { ...base, status: "completed", updatedAt: now };
    case "task.failed":
      return { ...base, status: "failed", updatedAt: now };
    default:
      return base;
  }
}

export function mergeSnapshot(view: TaskView, snapshot: TaskSnapshotDto): TaskView {
  const fresh = fromSnapshot(snapshot);
  return {
    ...fresh,
    interventions: view.interventions,
    notes: view.notes,
    connection: "closed",
    lastSeq: Math.max(fresh.lastSeq, view.lastSeq),
  };
}

export function withConnection(
  view: TaskView,
  connection: TaskView["connection"],
): TaskView {
  return view.connection === connection ? view : { ...view, connection };
}
