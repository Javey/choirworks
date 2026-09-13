import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { Composer } from "./Composer";
import { InterventionCard } from "./InterventionCard";
import { NodeCard } from "./NodeCard";
import type { NodeView } from "../lib/taskView";

describe("Composer", () => {
  it("sends on Enter and keeps Shift+Enter for newlines", async () => {
    const user = userEvent.setup();
    const onSend = vi.fn();
    render(<Composer disabled={false} hint="" onSend={onSend} />);

    const textarea = screen.getByPlaceholderText("输入消息，Enter 发送，Shift+Enter 换行");
    await user.type(textarea, "你好");
    await user.keyboard("{Shift>}{Enter}{/Shift}");
    expect(onSend).not.toHaveBeenCalled();
    await user.type(textarea, "世界");
    expect((textarea as HTMLTextAreaElement).value).toBe("你好\n世界");
    await user.keyboard("{Enter}");
    expect(onSend).toHaveBeenCalledWith("你好\n世界");
  });

  it("is disabled while a task is running", () => {
    render(<Composer disabled hint="任务执行中…" onSend={() => {}} />);
    expect(screen.getByPlaceholderText("任务执行中…")).toBeDisabled();
    expect(screen.getByRole("button", { name: "发送" })).toBeDisabled();
  });
});

describe("InterventionCard", () => {
  it("submits an answer", async () => {
    const user = userEvent.setup();
    const onAnswer = vi.fn().mockResolvedValue(undefined);
    render(
      <InterventionCard
        taskId="t1"
        intervention={{
          id: "iv1",
          status: "pending",
          policy: "human",
          source: "remote_input_required",
          questionText: "选哪个？",
        }}
        onAnswer={onAnswer}
      />,
    );
    expect(screen.getByText("选哪个？")).toBeInTheDocument();
    await user.type(screen.getByRole("textbox"), "选 A");
    await user.click(screen.getByRole("button", { name: "提交回答" }));
    expect(onAnswer).toHaveBeenCalledWith("t1", "iv1", "选 A");
  });

  it("renders resolved answers read-only", () => {
    render(
      <InterventionCard
        taskId="t1"
        intervention={{
          id: "iv1",
          status: "resolved",
          policy: "human",
          source: "remote_input_required",
          questionText: "选哪个？",
          answerText: "选 A",
          responder: "user",
        }}
        onAnswer={() => Promise.resolve()}
      />,
    );
    expect(screen.getByText("选 A")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "提交回答" })).not.toBeInTheDocument();
  });
});

describe("NodeCard", () => {
  const failed: NodeView = {
    id: "p1:n1",
    name: "n1",
    agentName: "echo",
    status: "failed",
    attempt: 2,
    error: "boom",
    order: 0,
  };

  it("offers retry for failed nodes", async () => {
    const user = userEvent.setup();
    const onRetry = vi.fn();
    render(<NodeCard node={failed} onRetry={onRetry} />);
    expect(screen.getByText("boom")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "重试" }));
    expect(onRetry).toHaveBeenCalledWith("p1:n1");
  });
});


describe("assist renderings", () => {
  it("shows assignee while a helper is working", () => {
    render(
      <InterventionCard
        taskId="t1"
        intervention={{
          id: "iv1",
          status: "pending",
          policy: "peer_agent",
          source: "remote_input_required",
          questionText: "缺少关键信息",
          assignedTo: "researcher",
        }}
        onAnswer={() => Promise.resolve()}
      />,
    );
    expect(screen.getByText(/已指派 researcher 处理中/)).toBeInTheDocument();
    expect(screen.queryByRole("textbox")).not.toBeInTheDocument();
  });

  it("marks derived nodes with an assist badge", () => {
    render(
      <NodeCard
        node={{
          id: "p1:a1",
          name: "协助 · researcher",
          agentName: "researcher",
          status: "working",
          attempt: 1,
          order: 1,
          derived: true,
        }}
        onRetry={() => {}}
      />,
    );
    expect(screen.getByText("协助")).toBeInTheDocument();
  });
});
