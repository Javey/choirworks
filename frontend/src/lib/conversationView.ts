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
  seq: number;
}

export interface SystemNotification {
  id: string;
  kind: string;
  text: string;
  agent_name?: string;
  node_id?: string;
  created_at: string;
  seq: number;
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

export interface InterventionInfo {
  id: string;
  status: string;
  node_id: string;
  kind: string;
  question?: string;
}

export interface ConversationView {
  taskId: string;
  contextId: string;
  state: string;
  messages: ChatMessage[];
  notifications: SystemNotification[];
  members: RoomMemberDto[];
  nodes: TaskNodeInfo[];
  interventions: Record<string, InterventionInfo>;
  activeArtifactIds: Set<string>;
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
  interventions: {},
  activeArtifactIds: new Set(),
  workingBubbles: [],
  lastSeq: 0,
};

type ProtoStruct = Record<string, unknown>;

const ROOM_META_KEY = "https://github.com/Javey/choirworks/extensions/room/v1";

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

const TERMINAL_NODE_STATUSES = new Set(["completed", "failed", "canceled", "invalidated"]);

function applyStateDelta(
  view: ConversationView,
  meta: ProtoStruct,
  state: string,
  seq: number,
): ConversationView {
  const notifications: SystemNotification[] = [];
  let nodes = view.nodes;
  let members = view.members;
  let interventions = view.interventions;

  // --- nodes ---
  const nodesDelta = meta.nodes as Record<string, ProtoStruct> | undefined;
  if (nodesDelta) {
    for (const [id, changes] of Object.entries(nodesDelta)) {
      const status = typeof changes.status === "string" ? changes.status : undefined;
      const existing = nodes.find((n) => n.id === id);
      if (existing) {
        nodes = nodes.map((n) => {
          if (n.id !== id) return n;
          return {
            ...n,
            status: status ?? n.status,
            output: typeof changes.output === "string" ? changes.output : n.output,
            error: typeof changes.error === "string" ? changes.error : n.error,
          };
        });
      } else {
        const newNode: TaskNodeInfo = {
          id,
          name: String(changes.name ?? changes.agent_name ?? ""),
          agent_name: String(changes.agent_name ?? ""),
          status: status ?? "pending",
          output: typeof changes.output === "string" ? changes.output : undefined,
          error: typeof changes.error === "string" ? changes.error : undefined,
        };
        nodes = [...nodes, newNode];
      }
      if (status && TERMINAL_NODE_STATUSES.has(status)) {
        // workingBubbles filtered below
      }
    }
  }

  // Clear working bubbles for any terminal nodes
  const terminalIds = new Set(
    nodes.filter((n) => TERMINAL_NODE_STATUSES.has(n.status)).map((n) => n.id),
  );
  const workingBubbles = terminalIds.size
    ? view.workingBubbles.filter((b) => !terminalIds.has(b.nodeId))
    : view.workingBubbles;

  // --- members ---
  const membersDelta = meta.members as ProtoStruct[] | undefined;
  if (membersDelta && membersDelta.length > 0) {
    const newMembers: RoomMemberDto[] = [];
    for (const m of membersDelta) {
      const agentName = String(m.agent_name ?? "");
      if (!agentName || members.some((mem) => mem.agent_name === agentName)) continue;
      newMembers.push({
        conversation_id: view.contextId,
        agent_name: agentName,
        agent_url: String(m.agent_url ?? ""),
        reason: typeof m.reason === "string" ? m.reason : null,
        joined_at: new Date().toISOString(),
      });
      notifications.push({
        id: `sys-join-${agentName}-${seq}`,
        kind: "room.participant_joined",
        text: `${agentName} 加入了群聊`,
        agent_name: agentName,
        created_at: new Date().toISOString(),
        seq,
      });
    }
    if (newMembers.length) {
      members = [...members, ...newMembers];
    }
  }

  // --- interventions ---
  const interventionsDelta = meta.interventions as Record<string, ProtoStruct> | undefined;
  if (interventionsDelta) {
    const newInterventions = { ...interventions };
    for (const [id, changes] of Object.entries(interventionsDelta)) {
      const status = typeof changes.status === "string" ? changes.status : "";
      const ivKind = typeof changes.kind === "string" ? changes.kind : "";
      const nodeId = typeof changes.node_id === "string" ? changes.node_id : "";
      const question = typeof changes.question === "string" ? changes.question : "";
      const prev = interventions[id];

      newInterventions[id] = {
        id,
        status: status || prev?.status || "pending",
        node_id: nodeId || prev?.node_id || "",
        kind: ivKind || prev?.kind || "question",
        question: question || prev?.question,
      };

      // Derive notification from state transition
      if (!prev && status === "pending" && ivKind === "confirm_cancel") {
        notifications.push({
          id: `sys-intervention-${id}`,
          kind: "intervention.requested",
          text: `待确认：${question}`,
          node_id: nodeId || undefined,
          created_at: new Date().toISOString(),
          seq,
        });
      } else if (status === "resolved" && (!prev || prev.status === "pending")) {
        notifications.push({
          id: `sys-intervention-${id}-resolved`,
          kind: "intervention.resolved",
          text: "人工答复已回填，任务继续",
          node_id: nodeId || prev?.node_id || undefined,
          created_at: new Date().toISOString(),
          seq,
        });
      } else if (status === "expired" && (!prev || prev.status === "pending")) {
        notifications.push({
          id: `sys-intervention-${id}-expired`,
          kind: "intervention.expired",
          text: "该确认已无需处理",
          node_id: nodeId || prev?.node_id || undefined,
          created_at: new Date().toISOString(),
          seq,
        });
      }
    }
    interventions = newInterventions;
  }

  return {
    ...view,
    state,
    nodes,
    members,
    interventions,
    workingBubbles,
    notifications: notifications.length
      ? [...view.notifications, ...notifications]
      : view.notifications,
    lastSeq: Math.max(view.lastSeq, seq),
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
        seq,
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
          seq,
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

    if (kind === "state_delta") {
      return applyStateDelta(view, meta, state, seq);
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
          seq,
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

    // Generic status update
    return { ...view, state, lastSeq: Math.max(view.lastSeq, seq) };
  }

  // Artifact update
  if (payload.$case === "artifactUpdate") {
    const artUpdate = result;
    const artifact = (artUpdate.artifact as ProtoStruct | undefined) ?? {};
    const meta = metaOf(artUpdate);
    const nodeId = typeof meta.node_id === "string" ? meta.node_id : null;
    const text = textOfParts(artifact);
    const lastChunk = artUpdate.lastChunk === true;
    const append = artUpdate.append === true;
    const agentName = typeof meta.agent_name === "string" ? meta.agent_name : "";
    const artifactId = String(artifact.artifactId ?? `art-${seq}`);
    const artifactMeta = metaOf(artifact);
    const author =
      typeof artifactMeta.author === "string"
        ? artifactMeta.author
        : agentName || "assistant";

    // Function call data part (Part.data with cw_type discriminator)
    const parts = (artifact.parts as ProtoStruct[] | undefined) ?? [];
    const pMeta = parts.length > 0 ? metaOf(parts[0]) : {};
    const firstPartContent = parts[0]?.content as { $case?: string; value?: unknown } | undefined;
    if (firstPartContent?.$case === "data" && pMeta.cw_type === "function_call") {
      const fcData = firstPartContent.value as ProtoStruct;
      const funcName = typeof fcData.function_name === "string" ? fcData.function_name : "";
      const funcArgs = (fcData.function_args as ProtoStruct | undefined) ?? {};
      const funcResult = (fcData.function_result as ProtoStruct | undefined) ?? {};
      const success = funcResult.success !== false;

      if (funcName === "create_plan") {
        if (!success) {
          const error = typeof funcResult.error === "string" ? funcResult.error : "计划失败";
          return {
            ...view,
            notifications: [...view.notifications, {
              id: `sys-plan-fail-${seq}`,
              kind: "plan.failed",
              text: `计划失败：${error}`,
              created_at: new Date().toISOString(),
            }],
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
        return {
          ...view,
          nodes,
          notifications: [...view.notifications, {
            id: `sys-plan-${seq}`,
            kind: "plan.created",
            text: `执行计划：${nodes.length} 个节点`,
            created_at: new Date().toISOString(),
          }],
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
        const partsText: string[] = [];
        if (invalidated.length > 0) partsText.push(`作废 ${invalidated.length} 个节点`);
        if (added.length > 0) partsText.push(`新增 ${added.length} 个节点`);
        return {
          ...view,
          nodes,
          notifications: [...view.notifications, {
            id: `sys-revised-${seq}`,
            kind: "plan.revised",
            text: `计划已修订：${partsText.join("，") || "无变化"}${reason ? `（${reason}）` : ""}`,
            created_at: new Date().toISOString(),
          }],
          lastSeq: Math.max(view.lastSeq, seq),
        };
      }

      if (funcName === "ask_user") {
        const nodeId = String(funcArgs.node_id ?? funcResult.node_id ?? "");
        const question = typeof (funcResult.question ?? funcArgs.question) === "string"
          ? String(funcResult.question ?? funcArgs.question)
          : "";
        const interventionId = String(funcResult.intervention_id ?? seq);
        return {
          ...view,
          state: taskStateToJSON(TaskState.TASK_STATE_INPUT_REQUIRED),
          nodes: view.nodes.map((n) =>
            n.id === nodeId ? { ...n, status: "input_required" } : n,
          ),
          notifications: [...view.notifications, {
            id: `sys-intervention-${interventionId}`,
            kind: "intervention.requested",
            text: question || "等待人工答复",
            node_id: nodeId || undefined,
            created_at: new Date().toISOString(),
          }],
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
        return {
          ...view,
          nodes,
          notifications: [...view.notifications, {
            id: `sys-assist-${seq}`,
            kind: "assist.dispatched",
            text: `${requester} 请求 ${helper} 协助`,
            created_at: new Date().toISOString(),
          }],
          lastSeq: Math.max(view.lastSeq, seq),
        };
      }
    }

    // Thought part in artifact (planner / agent reasoning stream)
    if (pMeta.cw_thought === true && text) {
      const chatMsg: ChatMessage = {
        id: artifactId,
        role: "assistant",
        sender: author,
        text,
        mentions: [],
        quote_id: null,
        node_id: nodeId,
        task_id: view.taskId,
        created_at: new Date().toISOString(),
        seq,
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

    // One-shot complete message (append=false + lastChunk=true, not part of a streaming sequence)
    const isStreaming = view.activeArtifactIds.has(artifactId);
    if (lastChunk && !isStreaming && text) {
      const chatMsg: ChatMessage = {
        id: artifactId,
        role: "agent",
        sender: agentName || null,
        text,
        mentions: [],
        quote_id: null,
        node_id: nodeId,
        task_id: view.taskId,
        created_at: new Date().toISOString(),
        seq,
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

    // Streaming working bubble (node output chunks)
    if (text) {
      const existing = view.workingBubbles.find((b) => b.nodeId === nodeId);
      const newText = append && existing ? existing.text + text : text;
      const activeArtifactIds = new Set(view.activeArtifactIds);
      if (!append && !lastChunk) {
        activeArtifactIds.add(artifactId);
      }
      if (lastChunk) {
        activeArtifactIds.delete(artifactId);
      }
      const updated = {
        ...view,
        activeArtifactIds,
        workingBubbles: existing
          ? view.workingBubbles.map((b) =>
            b.nodeId === nodeId ? { ...b, text: newText } : b,
          )
          : [...view.workingBubbles, { nodeId: nodeId ?? "", agentName, text: newText }],
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
      seq,
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
