import { useCallback, useEffect, useState } from "react";
import { X, AlertCircle } from "lucide-react";

import { api } from "./api/client";
import { ChatPanel } from "./components/ChatPanel";
import { DebugEventList } from "./components/DebugEventList";
import { RoomComposer } from "./components/room/RoomComposer";
import { Sidebar } from "./components/Sidebar";
import { useConversation, type ConversationSendInput } from "./hooks/useConversation";
import type { ConversationSummaryDto } from "./lib/types";

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : "请求失败";
}

export default function App() {
  const [conversations, setConversations] = useState<ConversationSummaryDto[]>([]);
  const [activeId, setActiveId] = useState<string | null>(() =>
    new URLSearchParams(window.location.search).get("c"),
  );
  const [banner, setBanner] = useState<string | null>(null);
  const [showDebug, setShowDebug] = useState(false);

  const { view, rawEvents, error, send, answerQuestion } = useConversation(activeId);

  const hasPendingQuestions = Object.values(view.questions).some(
    (question) => question.status === "pending",
  );

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
    async (input: ConversationSendInput) => {
      setBanner(null);
      const startingNew = activeId === null;
      try {
        await send(input, (contextId) => {
          if (!startingNew) return;
          navigate(contextId);
          void refreshConversations();
        });
        await refreshConversations();
      } catch (exc) {
        setBanner(messageOf(exc));
      }
    },
    [send, refreshConversations, activeId, navigate],
  );

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
          </div>
        ) : null}

        <header className="flex items-center justify-between gap-3 px-5 h-14 bg-white border-b border-feishu-border flex-shrink-0">
          <div className="flex items-center gap-2 min-w-0">
            <span className="font-semibold text-sm text-feishu-text truncate">
              {activeId
                ? (conversations.find((c) => c.id === activeId)?.title || "工作群")
                : "ChoirWorks"}
            </span>
          </div>
          <div className="flex items-center gap-1.5 flex-wrap">
            {view.members.map((member) => (
              <span
                key={member.agent_name}
                className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-feishu-bg border border-feishu-border text-[11px] text-feishu-text-secondary"
              >
                @{member.agent_name}
              </span>
            ))}
          </div>
        </header>

        {showDebug ? (
          <DebugEventList events={rawEvents} />
        ) : (
          <ChatPanel view={view} onAnswer={answerQuestion} />
        )}

        <div className="flex items-center justify-between px-6 py-1 bg-white border-t border-feishu-border flex-shrink-0">
          <button
            type="button"
            onClick={() => setShowDebug((v) => !v)}
            className="text-[11px] text-feishu-muted hover:text-feishu-text transition-colors"
          >
            {showDebug ? "返回对话" : "调试事件"}
          </button>
        </div>

        {hasPendingQuestions ? (
          <div className="px-6 py-3 bg-white border-t border-feishu-border flex-shrink-0 text-center text-[12px] text-feishu-muted">
            请先回答上方的问题卡片
          </div>
        ) : (
          <RoomComposer
            members={view.members}
            replyTo={null}
            interrupt={false}
            onToggleInterrupt={() => {}}
            onCancelReply={() => {}}
            onSend={handleSend}
          />
        )}
      </div>
    </div>
  );
}
