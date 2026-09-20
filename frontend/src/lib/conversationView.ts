import { Role, TaskState, taskStateToJSON } from "@a2a-js/sdk";
import type { EventDto, RoomMessageDto, RoomMemberDto } from "./types";

export interface ChatMessage {
  id: string;
  role: "user" | "assistant" | "agent" | "system";
  sender: string | null;
  text: string;
  mentions: string[];
  quote_id: string | null;
  node_id: string | null;
  task_id: string | null;
  created_at: string;
}

export interface SystemNotification {
  id: string;
  kind: string;
  text: string;
  agent_name?: string;
  node_id?: string;
  created_at: string;
}

export interface WorkingBubble {
  nodeId: string;
  agentName: string;
  text: string;
}

export interface TaskNodeInfo {
  id: string;
  name: string;
  agent_name: string;
  status: string;
  output?: string;
  error?: string;
}

export interface ConversationView {
  taskId: string;
  contextId: string;
  state: string;
  messages: ChatMessage[];
  notifications: SystemNotification[];
  members: RoomMemberDto[];
  nodes: TaskNodeInfo[];
  workingBubbles: WorkingBubble[];
  lastSeq: number;
}

export const emptyConversation: ConversationView = {
  taskId: "",
  contextId: "",
  state: taskStateToJSON(TaskState.TASK_STATE_SUBMITTED),
  messages: [],
  notifications: [],
  members: [],
  nodes: [],
  workingBubbles: [],
  lastSeq: 0,
};

type ProtoStruct = Record<string, unknown>;

const ROOM_META_KEY = "https://github.com/Javey/choirworks/extensions/room/v1";

function nodesFromContext(context: ProtoStruct | null | undefined): TaskNodeInfo[] {
  const nodesMeta = (context?.nodes as ProtoStruct[] | undefined) ?? [];
  return nodesMeta.map((n) => ({
    id: String(n.id ?? ""),
    name: String(n.name ?? ""),
    agent_name: String(n.agent_name ?? ""),
    status: String(n.status ?? "pending"),
    output: typeof n.output === "string" ? n.output : undefined,
    error: typeof n.error === "string" ? n.error : undefined,
  }));
}

function stateName(state: unknown): string {
  if (typeof state === "number") return taskStateToJSON(state as TaskState);
  if (typeof state === "string") return state;
  return taskStateToJSON(TaskState.TASK_STATE_UNSPECIFIED);
}

function textOfParts(container: ProtoStruct): string {
  const parts = (container.parts as ProtoStruct[] | undefined) ?? [];
  return parts
    .map((part) => {
      const content = part.content as { $case?: string; value?: unknown } | undefined;
      if (content?.$case === "text") return String(content.value ?? "");
      return "";
    })
    .join("\n");
}

function metaOf(container: ProtoStruct): ProtoStruct {
  return (container.metadata as ProtoStruct | undefined) ?? {};
}

function partMetaOf(container: ProtoStruct): ProtoStruct {
  const parts = (container.parts as ProtoStruct[] | undefined) ?? [];
  return parts.length > 0 ? metaOf(parts[0]) : {};
}

function roomMetaOf(container: ProtoStruct): ProtoStruct {
  const meta = metaOf(container);
  return (meta[ROOM_META_KEY] as ProtoStruct | undefined) ?? {};
}

export function conversationFromTask(
  task: ProtoStruct,
  taskId: string,
): ConversationView {
  return conversationFromTasks([task as ProtoStruct], taskId);
}

export function conversationFromTasks(
  tasks: ProtoStruct[],
  contextId: string,
  context?: ProtoStruct | null,
): ConversationView {
  const messages: ChatMessage[] = [];
  const seenIds = new Set<string>();
  for (const task of tasks) {
    const history = (task.history as ProtoStruct[] | undefined) ?? [];
    for (const msg of history) {
      const id = String(msg.messageId ?? "");
      if (seenIds.has(id)) continue;
      seenIds.add(id);
      const rm = roomMetaOf(msg);
      const sender = typeof rm.sender === "string" ? rm.sender : null;
      const role = msg.role === Role.ROLE_USER ? "user" : (sender === "assistant" ? "assistant" : "agent");
      messages.push({
        id,
        role,
        sender,
        text: textOfParts(msg),
        mentions: Array.isArray(rm.mentions) ? rm.mentions.map(String) : [],
        quote_id: typeof rm.quote_id === "string" ? rm.quote_id : null,
        node_id: typeof rm.node_id === "string" ? rm.node_id : null,
        task_id: typeof msg.taskId === "string" ? msg.taskId : null,
        created_at: "",
      });
    }
  }

  let lastTaskId = "";
  let lastState = taskStateToJSON(TaskState.TASK_STATE_SUBMITTED);
  const nodes = nodesFromContext(context);
  for (const task of tasks) {
    const state = stateName(
      (task.status as ProtoStruct | undefined)?.state,
    );
    lastTaskId = String(task.id ?? "");
    lastState = state;
  }

  const artifacts = (tasks.flatMap(
    (task) => (task.artifacts as ProtoStruct[] | undefined) ?? [],
  ));
  for (const art of artifacts) {
    const text = textOfParts(art);
    const nodeMeta = metaOf(art);
    const nodeId = typeof nodeMeta.node_id === "string" ? nodeMeta.node_id : null;
    if (nodeId && text) {
      const node = nodes.find((n) => n.id === nodeId);
      if (node && !node.output) {
        node.output = text;
      }
    }
  }

  return {
    taskId: lastTaskId,
    contextId: contextId,
    state: lastState,
    messages,
    notifications: [],
    members: [],
    nodes,
    workingBubbles: [],
    lastSeq: 0,
  };
}

export function applyStreamEvent(
  view: ConversationView,
  event: ProtoStruct,
  seq: number,
): ConversationView {
  const payload = event.payload as { $case?: string; value?: unknown } | undefined;
  if (!payload?.$case || payload.value === undefined) return view;
  const result = payload.value as ProtoStruct;

  // Task snapshot — merge new task's history into existing view
  if (payload.$case === "task") {
    const taskId = String(result.id ?? "");
    const state = stateName(
      (result.status as ProtoStruct | undefined)?.state,
    );
    const history = (result.history as ProtoStruct[] | undefined) ?? [];
    const newMessages: ChatMessage[] = [];
    const seenIds = new Set(view.messages.map((m) => m.id));
    for (const msg of history) {
      const id = String(msg.messageId ?? "");
      if (seenIds.has(id)) continue;
      seenIds.add(id);
      const rm = roomMetaOf(msg);
      const sender = typeof rm.sender === "string" ? rm.sender : null;
      const role = msg.role === Role.ROLE_USER ? "user" : (sender === "assistant" ? "assistant" : "agent");
      newMessages.push({
        id,
        role,
        sender,
        text: textOfParts(msg),
        mentions: Array.isArray(rm.mentions) ? rm.mentions.map(String) : [],
        quote_id: typeof rm.quote_id === "string" ? rm.quote_id : null,
        node_id: typeof rm.node_id === "string" ? rm.node_id : null,
        task_id: typeof msg.taskId === "string" ? msg.taskId : null,
        created_at: "",
      });
    }
    const nodes = view.nodes;
    return {
      ...view,
      taskId,
      state,
      messages: [...view.messages, ...newMessages],
      nodes,
      contextId: String(result.contextId ?? view.contextId),
      lastSeq: Math.max(view.lastSeq, seq),
    };
  }

  // Status update
  if (payload.$case === "statusUpdate") {
    const update = result;
    const meta = metaOf(update);
    const kind = typeof meta.kind === "string" ? meta.kind : "";
    const state = stateName(
      (update.status as ProtoStruct | undefined)?.state ?? view.state,
    );
    const statusMsg = (update.status as ProtoStruct | undefined)?.message as ProtoStruct | undefined;

    if (kind === "room.participant_joined") {
      const agentName = String(meta.agent_name ?? "");
      if (agentName && !view.members.some((m) => m.agent_name === agentName)) {
        const member: RoomMemberDto = {
          conversation_id: view.contextId,
          agent_name: agentName,
          agent_url: String(meta.agent_url ?? ""),
          reason: typeof meta.reason === "string" ? meta.reason : null,
          joined_at: new Date().toISOString(),
        };
        const notification: SystemNotification = {
          id: `sys-join-${agentName}-${seq}`,
          kind,
          text: `${agentName} 加入了群聊`,
          agent_name: agentName,
          created_at: new Date().toISOString(),
        };
        return {
          ...view,
          state,
          members: [...view.members, member],
          notifications: [...view.notifications, notification],
          lastSeq: Math.max(view.lastSeq, seq),
        };
      }
      return { ...view, state, lastSeq: Math.max(view.lastSeq, seq) };
    }

    if (kind === "function_call") {
      const funcName = typeof meta.function_name === "string" ? meta.function_name : "";
      const funcArgs = (meta.function_args as ProtoStruct | undefined) ?? {};
      const funcResult = (meta.function_result as ProtoStruct | undefined) ?? {};
      const success = funcResult.success !== false;

      if (funcName === "create_plan") {
        if (!success) {
          const error = typeof funcResult.error === "string" ? funcResult.error : "计划失败";
          const notification: SystemNotification = {
            id: `sys-plan-fail-${seq}`,
            kind: "plan.failed",
            text: `计划失败：${error}`,
            created_at: new Date().toISOString(),
          };
          return {
            ...view,
            state,
            notifications: [...view.notifications, notification],
            lastSeq: Math.max(view.lastSeq, seq),
          };
        }
        const nodesMeta = (funcArgs.nodes as ProtoStruct[] | undefined) ?? [];
        const nodes: TaskNodeInfo[] = nodesMeta.map((n: ProtoStruct) => ({
          id: String(n.id ?? ""),
          name: String(n.name ?? ""),
          agent_name: String(n.agent_name ?? ""),
          status: "pending",
        }));
        const notification: SystemNotification = {
          id: `sys-plan-${seq}`,
          kind: "plan.created",
          text: `执行计划：${nodes.length} 个节点`,
          created_at: new Date().toISOString(),
        };
        return {
          ...view,
          state,
          nodes,
          notifications: [...view.notifications, notification],
          lastSeq: Math.max(view.lastSeq, seq),
        };
      }

      if (funcName === "revise_plan") {
        const reason = typeof funcArgs.reason === "string" ? funcArgs.reason : "";
        const addedNodes = (funcResult.added_nodes as ProtoStruct[] | undefined) ?? [];
        const invalidated = Array.isArray(funcResult.invalidated)
          ? (funcResult.invalidated as unknown[]).map(String)
          : [];
        const added: TaskNodeInfo[] = addedNodes.map((n: ProtoStruct) => ({
          id: String(n.id ?? ""),
          name: String(n.name ?? ""),
          agent_name: String(n.agent_name ?? ""),
          status: "pending",
        }));
        const existing = new Set(view.nodes.map((n) => n.id));
        const nodes = [
          ...view.nodes.map((n) =>
            invalidated.includes(n.id) ? { ...n, status: "invalidated" } : n,
          ),
          ...added.filter((n) => !existing.has(n.id)),
        ];
        const parts: string[] = [];
        if (invalidated.length > 0) parts.push(`作废 ${invalidated.length} 个节点`);
        if (added.length > 0) parts.push(`新增 ${added.length} 个节点`);
        const notification: SystemNotification = {
          id: `sys-revised-${seq}`,
          kind: "plan.revised",
          text: `计划已修订：${parts.join("，") || "无变化"}${reason ? `（${reason}）` : ""}`,
          created_at: new Date().toISOString(),
        };
        return {
          ...view,
          state,
          nodes,
          notifications: [...view.notifications, notification],
          lastSeq: Math.max(view.lastSeq, seq),
        };
      }

      if (funcName === "ask_user") {
        const nodeId = String(funcArgs.node_id ?? funcResult.node_id ?? "");
        const question = typeof (funcResult.question ?? funcArgs.question) === "string"
          ? String(funcResult.question ?? funcArgs.question)
          : "";
        const interventionId = String(funcResult.intervention_id ?? seq);
        const notification: SystemNotification = {
          id: `sys-intervention-${interventionId}`,
          kind: "intervention.requested",
          text: question || "等待人工答复",
          node_id: nodeId || undefined,
          created_at: new Date().toISOString(),
        };
        return {
          ...view,
          state: taskStateToJSON(TaskState.TASK_STATE_INPUT_REQUIRED),
          nodes: view.nodes.map((n) =>
            n.id === nodeId ? { ...n, status: "input_required" } : n,
          ),
          notifications: [...view.notifications, notification],
          lastSeq: Math.max(view.lastSeq, seq),
        };
      }

      if (funcName === "call_subagent") {
        const helper = typeof funcResult.helper === "string" ? funcResult.helper : "";
        const requester = typeof funcResult.requester === "string" ? funcResult.requester : "";
        const helperNodeId = String(funcResult.helper_node_id ?? "");
        const node: TaskNodeInfo = {
          id: helperNodeId,
          name: helper,
          agent_name: helper,
          status: "pending",
        };
        const existing = new Set(view.nodes.map((n) => n.id));
        const nodes = existing.has(helperNodeId)
          ? view.nodes
          : [...view.nodes, node];
        const notification: SystemNotification = {
          id: `sys-assist-${seq}`,
          kind: "assist.dispatched",
          text: `${requester} 请求 ${helper} 协助`,
          created_at: new Date().toISOString(),
        };
        return {
          ...view,
          state,
          nodes,
          notifications: [...view.notifications, notification],
          lastSeq: Math.max(view.lastSeq, seq),
        };
      }

      return { ...view, state, lastSeq: Math.max(view.lastSeq, seq) };
    }

    if (kind === "plan.created") {
      const nodesMeta = (meta.nodes as ProtoStruct[] | undefined) ?? [];
      const nodes: TaskNodeInfo[] = nodesMeta.map((n) => ({
        id: String(n.id ?? ""),
        name: String(n.name ?? ""),
        agent_name: String(n.agent_name ?? ""),
        status: "pending",
      }));
      const notification: SystemNotification = {
        id: `sys-plan-${seq}`,
        kind,
        text: `执行计划：${nodes.length} 个节点`,
        created_at: new Date().toISOString(),
      };
      return {
        ...view,
        state,
        nodes,
        notifications: [...view.notifications, notification],
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    if (kind === "plan.revised") {
      const reason = typeof meta.reason === "string" ? meta.reason : "";
      const addedNodes = (meta.added_nodes as ProtoStruct[] | undefined) ?? [];
      const invalidated = Array.isArray(meta.invalidated)
        ? meta.invalidated.map(String)
        : [];
      const added: TaskNodeInfo[] = addedNodes.map((n) => ({
        id: String(n.id ?? ""),
        name: String(n.name ?? ""),
        agent_name: String(n.agent_name ?? ""),
        status: "pending",
      }));
      const existing = new Set(view.nodes.map((n) => n.id));
      const nodes = [
        ...view.nodes.map((n) =>
          invalidated.includes(n.id) ? { ...n, status: "invalidated" } : n,
        ),
        ...added.filter((n) => !existing.has(n.id)),
      ];
      const parts: string[] = [];
      if (invalidated.length > 0) parts.push(`作废 ${invalidated.length} 个节点`);
      if (added.length > 0) parts.push(`新增 ${added.length} 个节点`);
      const notification: SystemNotification = {
        id: `sys-revised-${seq}`,
        kind,
        text: `计划已修订：${parts.join("，") || "无变化"}${reason ? `（${reason}）` : ""}`,
        created_at: new Date().toISOString(),
      };
      return {
        ...view,
        state,
        nodes,
        notifications: [...view.notifications, notification],
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    if (kind === "node.invalidated" || kind === "node.canceled") {
      const nodeId = String(meta.node_id ?? "");
      const status = kind === "node.invalidated" ? "invalidated" : "canceled";
      return {
        ...view,
        state,
        nodes: view.nodes.map((n) => (n.id === nodeId ? { ...n, status } : n)),
        workingBubbles: view.workingBubbles.filter((b) => b.nodeId !== nodeId),
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    if (kind === "node.dispatched" || kind === "node.dispatch_intent") {
      const nodeId = String(meta.node_id ?? "");
      return {
        ...view,
        state,
        nodes: view.nodes.map((n) =>
          n.id === nodeId ? { ...n, status: "dispatched" } : n,
        ),
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    if (kind === "node.completed") {
      const nodeId = String(meta.node_id ?? "");
      const output = typeof meta.output_summary === "string" ? meta.output_summary : undefined;
      return {
        ...view,
        state,
        nodes: view.nodes.map((n) =>
          n.id === nodeId ? { ...n, status: "completed", output: output ?? n.output } : n,
        ),
        workingBubbles: view.workingBubbles.filter((b) => b.nodeId !== nodeId),
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    if (kind === "node.failed") {
      const nodeId = String(meta.node_id ?? "");
      const error = typeof meta.error === "string" ? meta.error : "failed";
      return {
        ...view,
        state,
        nodes: view.nodes.map((n) =>
          n.id === nodeId ? { ...n, status: "failed", error } : n,
        ),
        workingBubbles: view.workingBubbles.filter((b) => b.nodeId !== nodeId),
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    // StatusUpdate with message = thinking (assistant reasoning, plan.announced, etc.)
    if (statusMsg) {
      const msgText = textOfParts(statusMsg);
      if (msgText) {
        const rm = roomMetaOf(statusMsg);
        const sender = typeof rm.sender === "string" ? rm.sender : "assistant";
        const chatMsg: ChatMessage = {
          id: String(statusMsg.messageId ?? `thought-${seq}`),
          role: "assistant",
          sender,
          text: msgText,
          mentions: [],
          quote_id: null,
          node_id: typeof rm.node_id === "string" ? rm.node_id : null,
          task_id: view.taskId,
          created_at: new Date().toISOString(),
        };
        if (view.messages.some((m) => m.id === chatMsg.id)) {
          return view;
        }
        return {
          ...view,
          state,
          messages: [...view.messages, chatMsg],
          lastSeq: Math.max(view.lastSeq, seq),
        };
      }
    }

    if (
      kind === "intervention.requested" ||
      kind === "intervention.question" ||
      kind === "node.input_required"
    ) {
      const nodeId = String(meta.node_id ?? "");
      const question = typeof meta.question === "string" ? meta.question : "";
      const interventionKind =
        typeof meta.intervention_kind === "string" ? meta.intervention_kind : "";
      const isInputRequired = interventionKind !== "confirm_cancel";
      const notification: SystemNotification = {
        id: `sys-intervention-${String(meta.intervention_id ?? seq)}`,
        kind,
        text:
          interventionKind === "confirm_cancel"
            ? `待确认：${question}`
            : question || "等待人工答复",
        node_id: nodeId || undefined,
        created_at: new Date().toISOString(),
      };
      return {
        ...view,
        state: isInputRequired
          ? taskStateToJSON(TaskState.TASK_STATE_INPUT_REQUIRED)
          : state,
        nodes: isInputRequired
          ? view.nodes.map((n) =>
              n.id === nodeId ? { ...n, status: "input_required" } : n,
            )
          : view.nodes,
        notifications: [...view.notifications, notification],
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    if (kind === "intervention.resolved" || kind === "intervention.expired") {
      const notification: SystemNotification = {
        id: `sys-intervention-${String(meta.intervention_id ?? seq)}-${kind}`,
        kind,
        text:
          kind === "intervention.resolved"
            ? "人工答复已回填，任务继续"
            : "该确认已无需处理",
        node_id: typeof meta.node_id === "string" ? meta.node_id : undefined,
        created_at: new Date().toISOString(),
      };
      return {
        ...view,
        state,
        notifications: [...view.notifications, notification],
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    // Generic status update
    return { ...view, state, lastSeq: Math.max(view.lastSeq, seq) };
  }

  // Artifact update
  if (payload.$case === "artifactUpdate") {
    const artUpdate = result;
    const artifact = (artUpdate.artifact as ProtoStruct | undefined) ?? {};
    const meta = metaOf(artUpdate);
    const kind = typeof meta.kind === "string" ? meta.kind : "";
    const nodeId = typeof meta.node_id === "string" ? meta.node_id : null;
    const text = textOfParts(artifact);
    const lastChunk = artUpdate.lastChunk === true;
    const agentName = typeof meta.agent_name === "string" ? meta.agent_name : "";
    const artifactMeta = metaOf(artifact);
    const author =
      typeof artifactMeta.author === "string"
        ? artifactMeta.author
        : agentName || "assistant";

    // agent.message as artifact = agent output (final message, not streaming)
    if (kind === "agent.message" && text) {
      const chatMsg: ChatMessage = {
        id: String(artifact.artifactId ?? `agent-msg-${seq}`),
        role: "agent",
        sender: agentName || null,
        text,
        mentions: [],
        quote_id: null,
        node_id: nodeId,
        task_id: view.taskId,
        created_at: new Date().toISOString(),
      };
      if (view.messages.some((m) => m.id === chatMsg.id)) {
        return view;
      }
      return {
        ...view,
        messages: [...view.messages, chatMsg],
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    // Thought part in artifact (planner / agent reasoning stream)
    const pMeta = partMetaOf(artifact);
    if (pMeta.cw_thought === true && text) {
      const chatMsg: ChatMessage = {
        id: String(artifact.artifactId ?? `thought-${seq}`),
        role: "assistant",
        sender: author,
        text,
        mentions: [],
        quote_id: null,
        node_id: nodeId,
        task_id: view.taskId,
        created_at: new Date().toISOString(),
      };
      if (view.messages.some((m) => m.id === chatMsg.id)) {
        return view;
      }
      return {
        ...view,
        messages: [...view.messages, chatMsg],
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    // node.artifact = streaming output (working bubbles → final output)
    if (nodeId && text) {
      const existing = view.workingBubbles.find((b) => b.nodeId === nodeId);
      const append = artUpdate.append === true;
      const newText = append && existing ? existing.text + text : text;
      const updated = {
        ...view,
        workingBubbles: existing
          ? view.workingBubbles.map((b) =>
            b.nodeId === nodeId ? { ...b, text: newText } : b,
          )
          : [...view.workingBubbles, { nodeId, agentName, text: newText }],
        lastSeq: Math.max(view.lastSeq, seq),
      };
      if (lastChunk) {
        return {
          ...updated,
          workingBubbles: updated.workingBubbles.filter((b) => b.nodeId !== nodeId),
          nodes: view.nodes.map((n) =>
            n.id === nodeId ? { ...n, status: "working", output: newText } : n,
          ),
        };
      }
      return updated;
    }
    return { ...view, lastSeq: Math.max(view.lastSeq, seq) };
  }

  // Bare message
  if (payload.$case === "message") {
    const msg = result;
    const rm = roomMetaOf(msg);
    const chatMsg: ChatMessage = {
      id: String(msg.messageId ?? ""),
      role: msg.role === Role.ROLE_USER ? "user" : "agent",
      sender: typeof rm.sender === "string" ? rm.sender : null,
      text: textOfParts(msg),
      mentions: Array.isArray(rm.mentions) ? rm.mentions.map(String) : [],
      quote_id: typeof rm.quote_id === "string" ? rm.quote_id : null,
      node_id: typeof rm.node_id === "string" ? rm.node_id : null,
      task_id: typeof msg.taskId === "string" ? msg.taskId : null,
      created_at: new Date().toISOString(),
    };
    if (view.messages.some((m) => m.id === chatMsg.id)) {
      return view;
    }
    return {
      ...view,
      messages: [...view.messages, chatMsg],
      lastSeq: Math.max(view.lastSeq, seq),
    };
  }

  return view;
}

export type { EventDto, RoomMessageDto };
