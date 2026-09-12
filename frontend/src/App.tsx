import { useCallback, useEffect, useState } from "react";

import { api } from "./api/client";
import { Composer } from "./components/Composer";
import { RollbackDialog } from "./components/RollbackDialog";
import { Sidebar } from "./components/Sidebar";
import { Thread } from "./components/Thread";
import { useConversation } from "./hooks/useConversation";
import { pendingTaskView } from "./lib/taskView";
import type { ConversationSummaryDto, TaskStatus } from "./lib/types";

const TERMINAL: TaskStatus[] = ["completed", "failed", "canceled"];

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : "请求失败";
}

export default function App() {
  const [conversations, setConversations] = useState<ConversationSummaryDto[]>([]);
  const [activeId, setActiveId] = useState<string | null>(() =>
    new URLSearchParams(window.location.search).get("c"),
  );
  const { views, loading, error, load, appendTask, refreshTask } =
    useConversation(activeId);
  const [banner, setBanner] = useState<string | null>(null);
  const [rollbackFor, setRollbackFor] = useState<string | null>(null);

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
        const created = await api.createTask(text, activeId);
        if (!activeId) {
          navigate(created.conversation_id ?? null);
        } else {
          appendTask(pendingTaskView(created.task_id, text, created.conversation_id));
        }
        await refreshConversations();
      } catch (exc) {
        setBanner(messageOf(exc));
      }
    },
    [activeId, navigate, appendTask, refreshConversations],
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

  const latest = views.at(-1);
  const running = Boolean(latest && !TERMINAL.includes(latest.status));

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
        {loading && views.length === 0 ? (
          <div className="thread-empty">加载中…</div>
        ) : (
          <Thread
            views={views}
            onAnswer={answer}
            onRetry={retry}
            onCancel={cancel}
            onRollback={setRollbackFor}
          />
        )}
        <Composer disabled={running} hint={running ? "任务执行中…" : ""} onSend={send} />
      </div>
      {rollbackFor ? (
        <RollbackDialog
          taskId={rollbackFor}
          onClose={() => setRollbackFor(null)}
          onDone={() => {
            void load();
          }}
        />
      ) : null}
    </div>
  );
}
