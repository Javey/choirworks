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
  state: "TASK_STATE_SUBMITTED",
  messages: [],
  notifications: [],
  members: [],
  nodes: [],
  workingBubbles: [],
  lastSeq: 0,
};

type ProtoStruct = Record<string, unknown>;

function textOfParts(container: ProtoStruct): string {
  const parts = (container.parts as { text?: string }[] | undefined) ?? [];
  return parts.map((part) => part.text ?? "").join("\n");
}

function metaOf(container: ProtoStruct): ProtoStruct {
  return (container.metadata as ProtoStruct | undefined) ?? {};
}

export function conversationFromTask(
  task: ProtoStruct,
  taskId: string,
): ConversationView {
  const state = String(
    (task.status as ProtoStruct | undefined)?.state ?? "TASK_STATE_SUBMITTED",
  );
  const history = (task.history as ProtoStruct[] | undefined) ?? [];
  const messages: ChatMessage[] = history.map((msg) => {
    const meta = metaOf(msg);
    const role = msg.role === "ROLE_USER" ? "user" : "agent";
    const sender = typeof meta.sender === "string" ? meta.sender : null;
    const roomMeta = (meta["https://github.com/Javey/choirworks/extensions/room/v1"] as ProtoStruct | undefined) ?? {};
    return {
      id: String(msg.messageId ?? ""),
      role,
      sender,
      text: textOfParts(msg),
      mentions: Array.isArray(roomMeta.mentions) ? roomMeta.mentions.map(String) : [],
      quote_id: typeof roomMeta.quote_id === "string" ? roomMeta.quote_id : null,
      node_id: typeof roomMeta.node_id === "string" ? roomMeta.node_id : null,
      task_id: typeof msg.taskId === "string" ? msg.taskId : null,
      created_at: "",
    };
  });

  const meta = metaOf(task);
  const nodesMeta = (meta.nodes as ProtoStruct[] | undefined) ?? [];

  const nodes: TaskNodeInfo[] = nodesMeta.map((n) => ({
    id: String(n.id ?? ""),
    name: String(n.name ?? ""),
    agent_name: String(n.agent_name ?? ""),
    status: String(n.status ?? "pending"),
    output: typeof n.output === "string" ? n.output : undefined,
    error: typeof n.error === "string" ? n.error : undefined,
  }));

  const artifacts = (task.artifacts as ProtoStruct[] | undefined) ?? [];
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
    taskId,
    contextId: String(task.contextId ?? taskId),
    state,
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
  result: ProtoStruct,
  seq: number,
): ConversationView {
  // Task snapshot
  if (result.task) {
    return conversationFromTask(result.task as ProtoStruct, view.taskId || String((result.task as ProtoStruct).id ?? ""));
  }

  // Status update
  const update = result.statusUpdate as ProtoStruct | undefined;
  if (update) {
    const meta = metaOf(update);
    const kind = typeof meta.kind === "string" ? meta.kind : "";
    const state = String(
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

    if (kind === "agent.message" && statusMsg) {
      const msgText = textOfParts(statusMsg);
      const roomMeta = (metaOf(statusMsg)["https://github.com/Javey/choirworks/extensions/room/v1"] as ProtoStruct | undefined) ?? {};
      const sender = typeof roomMeta.sender === "string" ? roomMeta.sender : null;
      const nodeId = typeof roomMeta.node_id === "string" ? roomMeta.node_id : null;
      const chatMsg: ChatMessage = {
        id: String(statusMsg.messageId ?? `msg-${seq}`),
        role: "agent",
        sender,
        text: msgText,
        mentions: [],
        quote_id: null,
        node_id: nodeId,
        task_id: view.taskId,
        created_at: new Date().toISOString(),
      };
      return {
        ...view,
        state,
        messages: [...view.messages, chatMsg],
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    if (kind === "task.completion_summary" && statusMsg) {
      const msgText = textOfParts(statusMsg);
      const chatMsg: ChatMessage = {
        id: String(statusMsg.messageId ?? `completion-${seq}`),
        role: "assistant",
        sender: "assistant",
        text: msgText,
        mentions: [],
        quote_id: null,
        node_id: null,
        task_id: view.taskId,
        created_at: new Date().toISOString(),
      };
      return {
        ...view,
        state,
        messages: [...view.messages, chatMsg],
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    if (kind === "intervention.question" || kind === "node.input_required") {
      const nodeId = String(meta.node_id ?? "");
      return {
        ...view,
        state: "TASK_STATE_INPUT_REQUIRED",
        nodes: view.nodes.map((n) =>
          n.id === nodeId ? { ...n, status: "input_required" } : n,
        ),
        lastSeq: Math.max(view.lastSeq, seq),
      };
    }

    // Generic status update
    return { ...view, state, lastSeq: Math.max(view.lastSeq, seq) };
  }

  // Artifact update
  const artUpdate = result.artifactUpdate as ProtoStruct | undefined;
  if (artUpdate) {
    const artifact = (artUpdate.artifact as ProtoStruct | undefined) ?? {};
    const meta = metaOf(artUpdate);
    const nodeId = typeof meta.node_id === "string" ? meta.node_id : null;
    const text = textOfParts(artifact);
    const lastChunk = artUpdate.lastChunk === true;
    const agentName = typeof meta.agent_name === "string" ? meta.agent_name : "";

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
  if (result.message) {
    const msg = result.message as ProtoStruct;
    const meta = metaOf(msg);
    const roomMeta = (meta["https://github.com/Javey/choirworks/extensions/room/v1"] as ProtoStruct | undefined) ?? {};
    const chatMsg: ChatMessage = {
      id: String(msg.messageId ?? ""),
      role: msg.role === "ROLE_USER" ? "user" : "agent",
      sender: typeof roomMeta.sender === "string" ? roomMeta.sender : null,
      text: textOfParts(msg),
      mentions: Array.isArray(roomMeta.mentions) ? roomMeta.mentions.map(String) : [],
      quote_id: typeof roomMeta.quote_id === "string" ? roomMeta.quote_id : null,
      node_id: typeof roomMeta.node_id === "string" ? roomMeta.node_id : null,
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
