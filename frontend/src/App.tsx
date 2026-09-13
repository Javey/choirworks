import { useCallback, useEffect, useMemo, useState } from "react";

import { api } from "./api/client";
import { Composer } from "./components/Composer";
import { RollbackDialog } from "./components/RollbackDialog";
import { Sidebar } from "./components/Sidebar";
import { Thread } from "./components/Thread";
import { RoomComposer, type RoomSendInput } from "./components/room/RoomComposer";
import { RoomThread, type WorkingBubble } from "./components/room/RoomThread";
import { useConversation } from "./hooks/useConversation";
import { useRoom } from "./hooks/useRoom";
import type { ConversationSummaryDto, RoomMessageDto, TaskStatus } from "./lib/types";

const TERMINAL: TaskStatus[] = ["completed", "failed", "canceled"];
const WORKING_NODE_STATUSES = ["ready", "dispatched", "working"];

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : "请求失败";
}

export default function App() {
  const [conversations, setConversations] = useState<ConversationSummaryDto[]>([]);
  const [activeId, setActiveId] = useState<string | null>(() =>
    new URLSearchParams(window.location.search).get("c"),
  );
  const { views, error, load, refreshTask } = useConversation(activeId);
  const room = useRoom(activeId);
  const [banner, setBanner] = useState<string | null>(null);
  const [rollbackFor, setRollbackFor] = useState<string | null>(null);
  const [replyTo, setReplyTo] = useState<RoomMessageDto | null>(null);
  const [interrupt, setInterrupt] = useState(false);
  const [panelOpen, setPanelOpen] = useState(false);

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

  const send = useCallback(
    async (text: string) => {
      setBanner(null);
      try {
        const created = await api.createConversation(text.slice(0, 40));
        navigate(created.conversation_id);
        await api.postRoomMessage(created.conversation_id, { text });
        await refreshConversations();
      } catch (exc) {
        setBanner(messageOf(exc));
      }
    },
    [navigate, refreshConversations],
  );

  const sendRoom = useCallback(
    async (input: RoomSendInput) => {
      setBanner(null);
      try {
        await room.send(input);
        setReplyTo(null);
        setInterrupt(false);
        await refreshConversations();
      } catch (exc) {
        setBanner(messageOf(exc));
      }
    },
    [room, refreshConversations],
  );

  const answer = useCallback(
    async (taskId: string, interventionId: string, text: string) => {
      try {
        await api.answerIntervention(taskId, interventionId, text);
      } catch (exc) {
        setBanner(messageOf(exc));
      }
    },
    [],
  );

  const retry = useCallback(
    (taskId: string, nodeId: string) => {
      void api
        .retryNode(taskId, nodeId)
        .then(() => refreshTask(taskId))
        .catch((exc: unknown) => setBanner(messageOf(exc)));
    },
    [refreshTask],
  );

  const cancel = useCallback(
    (taskId: string) => {
      if (!window.confirm("确认取消该任务？")) return;
      void api
        .cancelTask(taskId)
        .then(() => refreshTask(taskId))
        .catch((exc: unknown) => setBanner(messageOf(exc)));
    },
    [refreshTask],
  );

  const workingBubbles = useMemo<WorkingBubble[]>(() => {
    const bubbles: WorkingBubble[] = [];
    for (const view of views) {
      if (TERMINAL.includes(view.status)) continue;
      for (const node of view.nodes) {
        if (WORKING_NODE_STATUSES.includes(node.status)) {
          bubbles.push({
            nodeId: node.id,
            agentName: node.agentName ?? node.name,
            text: node.outputText ?? "",
          });
        }
      }
    }
    return bubbles;
  }, [views]);

  const workingAgents = useMemo(
    () => new Set(workingBubbles.map((bubble) => bubble.agentName)),
    [workingBubbles],
  );

  const latest = views.at(-1);
  const running = Boolean(latest && !TERMINAL.includes(latest.status));
  const activeConversation = conversations.find((item) => item.id === activeId);

  return (
    <div className="app">
      <Sidebar
        conversations={conversations}
        activeId={activeId}
        onSelect={(id) => navigate(id)}
        onNew={() => navigate(null)}
      />
      <div className="content">
        {banner ? (
          <div className="banner">
            <span>{banner}</span>
            <button type="button" className="button small" onClick={() => setBanner(null)}>
              关闭
            </button>
          </div>
        ) : null}
        {error ? (
          <div className="banner">
            <span>{error}</span>
            <button type="button" className="button small" onClick={() => void load()}>
              重试
            </button>
          </div>
        ) : null}
        {activeId ? (
          <>
            <header className="room-head">
              <div className="room-head-title">
                <span>{activeConversation?.title ?? "工作群"}</span>
                {(room.error ?? room.loading) ? (
                  <span className="badge warn">
                    {room.error ? "消息加载失败" : "加载中…"}
                  </span>
                ) : null}
              </div>
              <div className="member-chips">
                {room.view.members.map((member) => (
                  <span key={member.agent_name} className="badge">
                    {workingAgents.has(member.agent_name) ? "● " : ""}@
                    {member.agent_name}
                  </span>
                ))}
              </div>
              <button
                type="button"
                className="button small"
                onClick={() => setPanelOpen((value) => !value)}
              >
                {panelOpen ? "收起详情" : "任务详情"}
              </button>
            </header>
            <div className="room-body">
              <div className="content">
                <RoomThread
                  view={room.view}
                  workingBubbles={workingBubbles}
                  onQuote={(message) => {
                    setReplyTo(message);
                    setInterrupt(false);
                  }}
                  onInterrupt={(message) => {
                    setReplyTo(message);
                    setInterrupt(true);
                  }}
                />
                <RoomComposer
                  members={room.view.members}
                  replyTo={replyTo}
                  interrupt={interrupt}
                  onToggleInterrupt={setInterrupt}
                  onCancelReply={() => {
                    setReplyTo(null);
                    setInterrupt(false);
                  }}
                  onSend={sendRoom}
                />
              </div>
              {panelOpen ? (
                <aside className="task-panel">
                  <Thread
                    views={views}
                    onAnswer={answer}
                    onRetry={retry}
                    onCancel={cancel}
                    onRollback={setRollbackFor}
                  />
                </aside>
              ) : null}
            </div>
          </>
        ) : (
          <>
            <Thread
              views={views}
              onAnswer={answer}
              onRetry={retry}
              onCancel={cancel}
              onRollback={setRollbackFor}
            />
            <Composer
              disabled={running}
              hint={running ? "任务执行中…" : ""}
              onSend={send}
            />
          </>
        )}
      </div>
      {rollbackFor ? (
        <RollbackDialog
          taskId={rollbackFor}
          onClose={() => setRollbackFor(null)}
          onDone={() => {
            void load();
            void room.load();
          }}
        />
      ) : null}
    </div>
  );
}
