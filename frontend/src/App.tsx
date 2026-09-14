import { useCallback, useEffect, useState } from "react";
import {
  X,
  PanelRightClose,
  PanelRightOpen,
  AlertCircle,
  Loader2,
} from "lucide-react";

import { api } from "./api/client";
import { Composer } from "./components/Composer";
import { Sidebar } from "./components/Sidebar";
import { RoomThread } from "./components/room/RoomThread";
import { RoomComposer } from "./components/room/RoomComposer";
import { useConversation, type ConversationSendInput } from "./hooks/useConversation";
import type { ConversationSummaryDto } from "./lib/types";
import type { ChatMessage } from "./lib/conversationView";

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : "请求失败";
}

function chatMessageToRoomMessage(msg: ChatMessage) {
  return {
    id: msg.id,
    conversation_id: "",
    seq: 0,
    role: msg.role,
    sender: msg.sender,
    text: msg.text,
    mentions: msg.mentions,
    quote_id: msg.quote_id,
    task_id: msg.task_id,
    node_id: msg.node_id,
    intervention_id: null,
    queued_for_node_id: null,
    delivered_at: null,
    created_at: msg.created_at,
  };
}

export default function App() {
  const [conversations, setConversations] = useState<ConversationSummaryDto[]>([]);
  const [activeId, setActiveId] = useState<string | null>(() =>
    new URLSearchParams(window.location.search).get("c"),
  );
  const [banner, setBanner] = useState<string | null>(null);
  const [panelOpen, setPanelOpen] = useState(false);

  const { view, connection, error, send, setOnTaskCreated } = useConversation(activeId);

  const refreshConversations = useCallback(async () => {
    try {
      setConversations(await api.listConversations());
    } catch (exc) {
      setBanner(messageOf(exc));
    }
  }, []);

  useEffect(() => {
    void refreshConversations();
  }, [refreshConversations]);

  useEffect(() => {
    const onPop = () =>
      setActiveId(new URLSearchParams(window.location.search).get("c"));
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  const navigate = useCallback((id: string | null) => {
    window.history.pushState({}, "", id ? `/?c=${id}` : "/");
    setActiveId(id);
  }, []);

  const handleSend = useCallback(
    async (text: string) => {
      setBanner(null);
      setOnTaskCreated((id) => navigate(id));
      try {
        await send({ text });
        await refreshConversations();
      } catch (exc) {
        setBanner(messageOf(exc));
      } finally {
        setOnTaskCreated(null);
      }
    },
    [send, navigate, refreshConversations, setOnTaskCreated],
  );

  const handleRoomSend = useCallback(
    async (input: ConversationSendInput) => {
      setBanner(null);
      try {
        await send(input);
        await refreshConversations();
      } catch (exc) {
        setBanner(messageOf(exc));
      }
    },
    [send, refreshConversations],
  );

  const isRunning = view.state === "TASK_STATE_WORKING";

  const workingBubbles = view.workingBubbles;
  const workingAgents = new Set(workingBubbles.map((b) => b.agentName));

  // Convert chat messages + notifications into a unified timeline
  const timelineMessages = [
    ...view.messages.map(chatMessageToRoomMessage),
    ...view.notifications.map((n) => ({
      id: n.id,
      conversation_id: "",
      seq: 0,
      role: "system" as const,
      sender: n.agent_name ?? null,
      text: n.text,
      mentions: [] as string[],
      quote_id: null,
      task_id: null,
      node_id: n.node_id ?? null,
      intervention_id: null,
      queued_for_node_id: null,
      delivered_at: null,
      created_at: n.created_at,
    })),
  ].sort((a, b) => (a.created_at < b.created_at ? -1 : a.created_at > b.created_at ? 1 : 0));

  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar
        conversations={conversations}
        activeId={activeId}
        onSelect={(id) => navigate(id)}
        onNew={() => navigate(null)}
      />

      <div className="flex-1 flex flex-col min-w-0 min-h-0">
        {banner ? (
          <div className="flex items-center justify-between gap-3 mx-6 mt-3 px-4 py-2.5 rounded-lg bg-feishu-danger-soft border border-feishu-danger/20 text-feishu-danger text-sm">
            <span className="flex items-center gap-2">
              <AlertCircle size={16} />
              {banner}
            </span>
            <button
              type="button"
              onClick={() => setBanner(null)}
              className="flex items-center justify-center w-5 h-5 rounded hover:bg-feishu-danger/10 transition-colors"
            >
              <X size={14} />
            </button>
          </div>
        ) : null}
        {error ? (
          <div className="flex items-center justify-between gap-3 mx-6 mt-3 px-4 py-2.5 rounded-lg bg-feishu-danger-soft border border-feishu-danger/20 text-feishu-danger text-sm">
            <span className="flex items-center gap-2">
              <AlertCircle size={16} />
              {error}
            </span>
            <button
              type="button"
              onClick={() => void useConversation}
              className="px-2.5 py-1 rounded text-xs font-medium hover:bg-feishu-danger/10 transition-colors"
            >
              重试
            </button>
          </div>
        ) : null}

        {activeId ? (
          <>
            <header className="flex items-center justify-between gap-3 px-5 h-14 bg-white border-b border-feishu-border flex-shrink-0">
              <div className="flex items-center gap-2 min-w-0">
                <span className="font-semibold text-sm text-feishu-text truncate">
                  {conversations.find((c) => c.id === activeId)?.title ?? "工作群"}
                </span>
                {connection === "reconnecting" ? (
                  <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-feishu-warn-soft text-[11px] text-feishu-warn border border-feishu-warn/20">
                    <Loader2 size={10} className="animate-spin" />
                    重连中…
                  </span>
                ) : null}
              </div>
              <div className="flex items-center gap-1.5 flex-wrap">
                {view.members.map((member) => (
                  <span
                    key={member.agent_name}
                    className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-feishu-bg border border-feishu-border text-[11px] text-feishu-text-secondary"
                  >
                    {workingAgents.has(member.agent_name) ? (
                      <span className="w-1.5 h-1.5 rounded-full bg-feishu-success animate-pulse" />
                    ) : null}
                    @{member.agent_name}
                  </span>
                ))}
              </div>
              <button
                type="button"
                onClick={() => setPanelOpen((value) => !value)}
                className="flex items-center justify-center w-8 h-8 rounded-lg text-feishu-muted hover:bg-feishu-bg hover:text-feishu-text transition-colors"
                title={panelOpen ? "收起详情" : "任务详情"}
              >
                {panelOpen ? <PanelRightClose size={18} /> : <PanelRightOpen size={18} />}
              </button>
            </header>

            <div className="flex-1 flex flex-col min-w-0 min-h-0 overflow-hidden">
              <RoomThread
                view={{
                  messages: timelineMessages,
                  members: view.members,
                  summary: null,
                  lastSeq: view.lastSeq,
                }}
                workingBubbles={workingBubbles}
                onQuote={() => {}}
                onInterrupt={() => {}}
              />
              <RoomComposer
                members={view.members}
                replyTo={null}
                interrupt={false}
                onToggleInterrupt={() => {}}
                onCancelReply={() => {}}
                onSend={handleRoomSend}
              />
            </div>

            {panelOpen ? (
              <aside className="w-[400px] min-w-[360px] min-h-0 border-l border-feishu-border bg-white flex flex-col overflow-hidden p-4">
                <h3 className="font-semibold text-sm mb-3">任务详情</h3>
                <div className="flex-1 overflow-y-auto">
                  <div className="space-y-2">
                    <div className="text-xs text-feishu-muted">状态: {view.state}</div>
                    {view.nodes.map((node) => (
                      <div key={node.id} className="p-2 rounded-lg border border-feishu-border text-xs">
                        <div className="font-medium">{node.name}</div>
                        <div className="text-feishu-muted">@{node.agent_name} · {node.status}</div>
                        {node.output ? (
                          <div className="mt-1 text-feishu-text-secondary truncate">{node.output.slice(0, 80)}</div>
                        ) : null}
                        {node.error ? (
                          <div className="mt-1 text-feishu-danger">{node.error}</div>
                        ) : null}
                      </div>
                    ))}
                  </div>
                </div>
              </aside>
            ) : null}
          </>
        ) : (
          <>
            <div className="flex-1 flex flex-col items-center justify-center py-20 text-center">
              <div className="flex items-center justify-center w-16 h-16 rounded-full bg-feishu-primary-soft mb-4">
                <PanelRightOpen size={28} className="text-feishu-primary" />
              </div>
              <h2 className="text-lg font-bold text-feishu-text mb-2">
                开始协作
              </h2>
              <p className="text-sm text-feishu-muted max-w-xs leading-relaxed mb-4">
                描述你的目标，平台会拆解任务并把 Agent 拉进群里。
              </p>
            </div>
            <Composer
              disabled={isRunning}
              hint={isRunning ? "任务执行中…" : ""}
              onSend={handleSend}
            />
          </>
        )}
      </div>
    </div>
  );
}
