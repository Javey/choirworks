import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { useConversation } from "./useConversation";
import { getClient } from "../api/a2a-client";
import { api } from "../api/client";

vi.mock("../api/a2a-client", () => ({ getClient: vi.fn() }));
vi.mock("../api/client", () => ({ api: { getConversationEvents: vi.fn() } }));

const taskEvent = {
  payload: {
    $case: "task",
    value: {
      id: "task-1",
      contextId: "ctx-1",
      status: { state: 2 },
      history: [],
      artifacts: [],
    },
  },
};

interface StreamHandle {
  signal: AbortSignal | undefined;
  push: (event: unknown) => void;
  finish: () => void;
}

function makeControlledStream(signal: AbortSignal | undefined) {
  const queue: unknown[] = [];
  let wake: (() => void) | null = null;
  let finished = false;
  const abortError = () => Object.assign(new Error("aborted"), { name: "AbortError" });

  async function* generator(): AsyncGenerator<unknown, void, unknown> {
    try {
      while (true) {
        if (queue.length > 0) {
          yield queue.shift();
          continue;
        }
        if (finished) return;
        if (signal?.aborted) throw abortError();
        const aborted = signal
          ? new Promise<never>((_, reject) => {
              signal.addEventListener("abort", () => reject(abortError()), { once: true });
            })
          : null;
        const wait = new Promise<void>((resolve) => {
          wake = resolve;
        });
        if (aborted) {
          await Promise.race([wait, aborted]);
        } else {
          await wait;
        }
      }
    } finally {
      finished = true;
    }
  }

  return {
    generator,
    push: (event: unknown) => {
      queue.push(event);
      wake?.();
      wake = null;
    },
    finish: () => {
      finished = true;
      wake?.();
      wake = null;
    },
  };
}

function makeFakeClient() {
  const sendStreams: StreamHandle[] = [];
  const followStreams: StreamHandle[] = [];

  const client = {
    sendMessageStream: vi.fn((_params: unknown, options?: { signal?: AbortSignal }) => {
      const stream = makeControlledStream(options?.signal);
      sendStreams.push({ signal: options?.signal, push: stream.push, finish: stream.finish });
      return stream.generator();
    }),
    resubscribeTask: vi.fn((_params: unknown, options?: { signal?: AbortSignal }) => {
      const stream = makeControlledStream(options?.signal);
      followStreams.push({ signal: options?.signal, push: stream.push, finish: stream.finish });
      return stream.generator();
    }),
  };
  return {
    client: client as unknown as Awaited<ReturnType<typeof getClient>>,
    rawClient: client,
    sendStreams,
    followStreams,
  };
}

function fireSend(result: { current: ReturnType<typeof useConversation> }, text: string) {
  void result.current.send({ text }).catch(() => {});
}

describe("useConversation single live stream", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("send aborts the active follow before opening its own stream", async () => {
    const { client, rawClient, sendStreams, followStreams } = makeFakeClient();
    vi.mocked(getClient).mockResolvedValue(client);

    const { result } = renderHook(() => useConversation("ctx-1"));
    await waitFor(() => expect(api.getConversationEvents).toHaveBeenCalledWith("ctx-1"));

    await act(async () => {
      fireSend(result, "first");
    });
    await waitFor(() => expect(sendStreams.length).toBe(1));
    act(() => {
      sendStreams[0].push(taskEvent);
      sendStreams[0].finish();
    });
    await waitFor(() => expect(rawClient.resubscribeTask).toHaveBeenCalledTimes(1));
    expect(followStreams[0]?.signal?.aborted).toBe(false);

    act(() => {
      fireSend(result, "second");
    });
    expect(followStreams[0]?.signal?.aborted).toBe(true);

    await waitFor(() => expect(sendStreams.length).toBe(2));
    act(() => {
      sendStreams[1].push(taskEvent);
      sendStreams[1].finish();
    });
    await waitFor(() => expect(rawClient.resubscribeTask).toHaveBeenCalledTimes(2));
    expect(followStreams[1]?.signal?.aborted).toBe(false);
  });

  it("follow consumes events while it is the only live stream", async () => {
    const { client, rawClient, sendStreams, followStreams } = makeFakeClient();
    vi.mocked(getClient).mockResolvedValue(client);

    const { result } = renderHook(() => useConversation("ctx-1"));
    await waitFor(() => expect(api.getConversationEvents).toHaveBeenCalledWith("ctx-1"));

    await act(async () => {
      fireSend(result, "first");
    });
    await waitFor(() => expect(sendStreams.length).toBe(1));
    act(() => {
      sendStreams[0].push(taskEvent);
      sendStreams[0].finish();
    });
    await waitFor(() => expect(rawClient.resubscribeTask).toHaveBeenCalledTimes(1));
    expect(result.current.rawEvents.length).toBe(1);

    act(() => {
      followStreams[0].push(taskEvent);
    });
    await waitFor(() => expect(result.current.rawEvents.length).toBe(2));
  });

  it("switching conversations aborts the in-flight send and it starts no follow", async () => {
    const { client, rawClient, sendStreams } = makeFakeClient();
    vi.mocked(getClient).mockResolvedValue(client);

    const { result, rerender } = renderHook(({ contextId }) => useConversation(contextId), {
      initialProps: { contextId: "ctx-1" as string | null },
    });
    await waitFor(() => expect(api.getConversationEvents).toHaveBeenCalledWith("ctx-1"));

    await act(async () => {
      fireSend(result, "first");
    });
    await waitFor(() => expect(sendStreams.length).toBe(1));
    act(() => {
      sendStreams[0].push(taskEvent);
    });
    await waitFor(() => expect(result.current.view.taskId).toBe("task-1"));

    rerender({ contextId: "ctx-2" });
    await waitFor(() => expect(api.getConversationEvents).toHaveBeenCalledWith("ctx-2"));
    expect(sendStreams[0]?.signal?.aborted).toBe(true);

    await act(async () => {});
    expect(rawClient.resubscribeTask).not.toHaveBeenCalled();
  });

  it("switching conversations aborts the active follow", async () => {
    const { client, rawClient, sendStreams, followStreams } = makeFakeClient();
    vi.mocked(getClient).mockResolvedValue(client);

    const { result, rerender } = renderHook(({ contextId }) => useConversation(contextId), {
      initialProps: { contextId: "ctx-1" as string | null },
    });
    await waitFor(() => expect(api.getConversationEvents).toHaveBeenCalledWith("ctx-1"));

    await act(async () => {
      fireSend(result, "first");
    });
    await waitFor(() => expect(sendStreams.length).toBe(1));
    act(() => {
      sendStreams[0].push(taskEvent);
      sendStreams[0].finish();
    });
    await waitFor(() => expect(rawClient.resubscribeTask).toHaveBeenCalledTimes(1));

    rerender({ contextId: "ctx-2" });
    await waitFor(() => expect(api.getConversationEvents).toHaveBeenCalledWith("ctx-2"));
    expect(followStreams[0]?.signal?.aborted).toBe(true);
    expect(rawClient.resubscribeTask).toHaveBeenCalledTimes(1);
  });

  it("unmount aborts the active follow", async () => {
    const { client, rawClient, sendStreams, followStreams } = makeFakeClient();
    vi.mocked(getClient).mockResolvedValue(client);

    const { result, unmount } = renderHook(() => useConversation("ctx-1"));
    await waitFor(() => expect(api.getConversationEvents).toHaveBeenCalledWith("ctx-1"));

    await act(async () => {
      fireSend(result, "first");
    });
    await waitFor(() => expect(sendStreams.length).toBe(1));
    act(() => {
      sendStreams[0].push(taskEvent);
      sendStreams[0].finish();
    });
    await waitFor(() => expect(rawClient.resubscribeTask).toHaveBeenCalledTimes(1));
    expect(followStreams[0]?.signal?.aborted).toBe(false);

    unmount();
    expect(followStreams[0]?.signal?.aborted).toBe(true);
  });
});
