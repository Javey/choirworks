import { useCallback, useEffect, useRef, useState } from "react";

import { api } from "../api/client";
import { subscribeTaskEvents } from "../api/events";
import {
  applyEvent,
  fromSnapshot,
  isTerminal,
  mergeSnapshot,
  withConnection,
  type TaskView,
} from "../lib/taskView";
import type { TaskStatus } from "../lib/types";

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : "请求失败";
}

export function useConversation(conversationId: string | null) {
  const [views, setViews] = useState<TaskView[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const streams = useRef(new Map<string, () => void>());
  const observed = useRef(new Map<string, TaskStatus>());
  const version = useRef(0);

  const load = useCallback(async () => {
    const current = ++version.current;
    if (!conversationId) {
      setViews([]);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const detail = await api.getConversation(conversationId);
      if (current === version.current) setViews(detail.tasks.map(fromSnapshot));
    } catch (exc) {
      if (current === version.current) setError(messageOf(exc));
    } finally {
      if (current === version.current) setLoading(false);
    }
  }, [conversationId]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    const activeStreams = streams.current;
    return () => {
      for (const close of activeStreams.values()) close();
      activeStreams.clear();
      observed.current.clear();
    };
  }, [conversationId]);

  useEffect(() => {
    for (const view of views) {
      const terminal = isTerminal(view.status);
      if (!terminal && !streams.current.has(view.id)) {
        const close = subscribeTaskEvents(view.id, view.lastSeq, {
          onEvent: (event) =>
            setViews((prev) =>
              prev.map((item) => (item.id === view.id ? applyEvent(item, event) : item)),
            ),
          onState: (state) =>
            setViews((prev) =>
              prev.map((item) => (item.id === view.id ? withConnection(item, state) : item)),
            ),
        });
        streams.current.set(view.id, close);
      }
      if (terminal) {
        const close = streams.current.get(view.id);
        if (close) {
          close();
          streams.current.delete(view.id);
        }
        if (observed.current.get(view.id) !== view.status) {
          observed.current.set(view.id, view.status);
          void api
            .getTask(view.id)
            .then((snapshot) =>
              setViews((prev) =>
                prev.map((item) =>
                  item.id === view.id ? mergeSnapshot(item, snapshot) : item,
                ),
              ),
            )
            .catch(() => undefined);
        }
      } else {
        observed.current.set(view.id, view.status);
      }
    }
  }, [views]);

  const appendTask = useCallback((view: TaskView) => {
    setViews((prev) => [...prev, view]);
  }, []);

  const refreshTask = useCallback(async (taskId: string) => {
    const snapshot = await api.getTask(taskId);
    setViews((prev) =>
      prev.map((item) => (item.id === taskId ? mergeSnapshot(item, snapshot) : item)),
    );
  }, []);

  return { views, loading, error, load, appendTask, refreshTask };
}
