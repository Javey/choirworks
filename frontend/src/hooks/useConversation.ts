import { useCallback, useEffect, useRef, useState } from "react";

import { Role, TaskState, taskStateToJSON } from "@a2a-js/sdk";

import type { Task } from "@a2a-js/sdk";
import type { StreamResponse } from "@a2a-js/sdk";

import { getClient } from "../api/a2a-client";
import {
  applyStreamEvent,
  conversationFromTask,
  emptyConversation,
  type ConversationView,
} from "../lib/conversationView";

export interface ConversationSendInput {
  text: string;
  mentions?: string[];
  quote_id?: string;
  interrupt?: boolean;
}

const SETTLED_STATES = new Set([
  taskStateToJSON(TaskState.TASK_STATE_COMPLETED),
  taskStateToJSON(TaskState.TASK_STATE_FAILED),
  taskStateToJSON(TaskState.TASK_STATE_CANCELED),
  taskStateToJSON(TaskState.TASK_STATE_REJECTED),
  taskStateToJSON(TaskState.TASK_STATE_INPUT_REQUIRED),
]);

function isSettled(state: string | undefined): boolean {
  return state !== undefined && SETTLED_STATES.has(state);
}

export function useConversation(conversationId: string | null) {
  const [view, setView] = useState<ConversationView>(emptyConversation);
  const [rawEvents, setRawEvents] = useState<unknown[]>([]);
  const [error, setError] = useState<string | null>(null);
  const navigateRef = useRef<((id: string) => void) | null>(null);
  const viewRef = useRef(view);
  const seqRef = useRef(0);
  const genRef = useRef(0);

  const commit = useCallback((next: ConversationView) => {
    viewRef.current = next;
    setView(next);
  }, []);

  const apply = useCallback(
    (event: unknown) => {
      const seq = seqRef.current++;
      setRawEvents((prev) => [...prev, event]);
      commit(
        applyStreamEvent(
          viewRef.current,
          event as unknown as Record<string, unknown>,
          seq,
        ),
      );
    },
    [commit],
  );

  const follow = useCallback(
    async (taskId: string) => {
      const generation = genRef.current;
      const client = await getClient();
      try {
        for await (const event of client.resubscribeTask({
          tenant: "",
          id: taskId,
        })) {
          if (generation !== genRef.current) return;
          apply(event);
        }
      } catch {
        // Task may already be terminal or cleaned up; the snapshot stays valid.
      }
    },
    [apply],
  );

  useEffect(() => {
    if (viewRef.current.taskId === conversationId) return;

    genRef.current += 1;
    seqRef.current = 0;
    setRawEvents([]);
    if (!conversationId) {
      commit(emptyConversation);
      return;
    }
    const generation = genRef.current;
    void (async () => {
      const client = await getClient();
      try {
        const task = (await client.getTask({
          tenant: "",
          id: conversationId,
        })) as unknown as Task;
        if (generation !== genRef.current) return;
        const snapshot = conversationFromTask(
          task as unknown as Record<string, unknown>,
          conversationId,
        );
        commit(snapshot);
        if (!isSettled(snapshot.state)) {
          await follow(conversationId);
        }
      } catch (exc) {
        if (generation === genRef.current) {
          setError(exc instanceof Error ? exc.message : "加载会话失败");
        }
      }
    })();
  }, [conversationId, follow, commit]);

  const send = useCallback(
    async (input: ConversationSendInput) => {
      const client = await getClient();
      const current = viewRef.current;
      const settled = isSettled(current.state);
      const taskId = settled
        ? ""
        : current.taskId || conversationId || "";
      const contextId = current.contextId || conversationId || "";
      const message = {
        messageId: crypto.randomUUID(),
        role: Role.ROLE_USER,
        parts: [
          {
            content: { $case: "text" as const, value: input.text },
            metadata: undefined,
            filename: "",
            mediaType: "text/plain",
          },
        ],
        contextId,
        taskId,
        metadata: undefined,
        extensions: [],
        referenceTaskIds: [],
      };

      let createdTaskId = taskId;
      for await (const event of client.sendMessageStream({
        message,
        tenant: "",
        configuration: undefined,
        metadata: undefined,
      })) {
        apply(event);
        const payload = (event as StreamResponse).payload;
        if (payload?.$case === "task" && payload.value?.id) {
          createdTaskId = payload.value.id;
          if (navigateRef.current) {
            navigateRef.current(payload.value.id);
            navigateRef.current = null;
          }
        }
      }
      const latest = viewRef.current;
      if (createdTaskId && !isSettled(latest.state)) {
        await follow(createdTaskId);
      }
    },
    [conversationId, apply, follow],
  );

  const setOnTaskCreated = useCallback((fn: ((id: string) => void) | null) => {
    navigateRef.current = fn;
  }, []);

  return { view, rawEvents, error, send, setOnTaskCreated };
}
