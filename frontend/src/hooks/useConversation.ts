import { useCallback, useEffect, useRef, useState } from "react";

import { Role, StreamResponse, TaskState, taskStateToJSON } from "@a2a-js/sdk";
import type { Part } from "@a2a-js/sdk";

import { getClient } from "../api/a2a-client";
import { api } from "../api/client";
import {
  applyStreamEvent,
  emptyConversation,
  type QuestionInfo,
  type ConversationView,
} from "../lib/conversationView";

export interface ConversationSendInput {
  text: string;
  mentions?: string[];
  quote_id?: string;
  interrupt?: boolean;
  interventionId?: string;
  answer?: string | string[] | boolean;
}

const SETTLED_STATES = new Set([
  taskStateToJSON(TaskState.TASK_STATE_COMPLETED),
  taskStateToJSON(TaskState.TASK_STATE_FAILED),
  taskStateToJSON(TaskState.TASK_STATE_CANCELED),
  taskStateToJSON(TaskState.TASK_STATE_REJECTED),
]);

function isSettled(state: string | undefined): boolean {
  return state !== undefined && SETTLED_STATES.has(state);
}

export function useConversation(contextId: string | null) {
  const [view, setView] = useState<ConversationView>(emptyConversation);
  const [rawEvents, setRawEvents] = useState<unknown[]>([]);
  const [error, setError] = useState<string | null>(null);
  const viewRef = useRef(view);
  const seqRef = useRef(0);
  const genRef = useRef(0);
  const followAbortRef = useRef<AbortController | null>(null);
  const sendAbortRef = useRef<AbortController | null>(null);

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
      followAbortRef.current?.abort();
      const controller = new AbortController();
      followAbortRef.current = controller;
      const generation = genRef.current;
      const client = await getClient();
      try {
        for await (const event of client.resubscribeTask(
          { tenant: "", id: taskId },
          { signal: controller.signal },
        )) {
          if (generation !== genRef.current) return;
          apply(event);
        }
      } catch {
        // Task may already be terminal or cleaned up; the snapshot stays valid.
      } finally {
        if (followAbortRef.current === controller) {
          followAbortRef.current = null;
        }
      }
    },
    [apply],
  );

  useEffect(() => {
    if (viewRef.current.contextId !== contextId) {
      genRef.current += 1;
      seqRef.current = 0;
      setRawEvents([]);
      if (!contextId) {
        commit(emptyConversation);
      } else {
        const generation = genRef.current;
        void (async () => {
          try {
            const events = await api.getConversationEvents(contextId);
            if (generation !== genRef.current) return;
            for (const raw of events) {
              apply(StreamResponse.fromJSON(raw));
            }
            const snapshot = viewRef.current;
            if (snapshot.taskId && !isSettled(snapshot.state)) {
              await follow(snapshot.taskId);
            }
          } catch (exc) {
            if (generation === genRef.current) {
              setError(exc instanceof Error ? exc.message : "加载会话失败");
            }
          }
        })();
      }
    }
    return () => {
      followAbortRef.current?.abort();
      sendAbortRef.current?.abort();
    };
  }, [contextId, follow, commit]);

  const send = useCallback(
    async (
      input: ConversationSendInput,
      onContextCreated?: (contextId: string) => void,
    ) => {
      followAbortRef.current?.abort();
      const client = await getClient();
      const current = viewRef.current;
      const settled = isSettled(current.state);
      const taskId = settled ? "" : current.taskId;
      let knownContextId = current.contextId;
      const parts: Part[] = [];
      if (input.text) {
        parts.push({
          content: { $case: "text", value: input.text },
          metadata: undefined,
          filename: "",
          mediaType: "text/plain",
        });
      }
      if (input.interventionId !== undefined && input.answer !== undefined) {
        parts.push({
          content: {
            $case: "data",
            value: { intervention_id: input.interventionId, answer: input.answer },
          },
          metadata: { cw_type: "question_response" },
          filename: "",
          mediaType: "",
        });
      }
      const message = {
        messageId: crypto.randomUUID(),
        role: Role.ROLE_USER,
        parts,
        contextId: knownContextId,
        taskId,
        metadata: undefined,
        extensions: [],
        referenceTaskIds: [],
      };

      const controller = new AbortController();
      sendAbortRef.current = controller;
      const generation = genRef.current;
      let createdTaskId = taskId;
      try {
        for await (const event of client.sendMessageStream(
          {
            message,
            tenant: "",
            configuration: undefined,
            metadata: undefined,
          },
          { signal: controller.signal },
        )) {
          if (generation !== genRef.current) return;
          apply(event);
          const payload = (event as StreamResponse).payload;
          if (payload?.$case === "task" && payload.value?.id) {
            createdTaskId = payload.value.id as string;
            const createdContextId = payload.value.contextId;
            if (createdContextId && createdContextId !== knownContextId) {
              knownContextId = createdContextId;
              onContextCreated?.(createdContextId);
            }
          }
        }
      } catch (exc) {
        if ((exc as { name?: string } | null)?.name !== "AbortError") {
          throw exc;
        }
      } finally {
        if (sendAbortRef.current === controller) {
          sendAbortRef.current = null;
        }
      }
      if (controller.signal.aborted || generation !== genRef.current) return;
      const latest = viewRef.current;
      if (createdTaskId && !isSettled(latest.state)) {
        await follow(createdTaskId);
      }
    },
    [apply, follow],
  );

  const answerQuestion = useCallback(
    async (question: QuestionInfo, answer: string | string[] | boolean, text: string) => {
      await send({ text, interventionId: question.id, answer });
    },
    [send],
  );

  return { view, rawEvents, error, send, answerQuestion };
}
