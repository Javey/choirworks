import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { emptyConversation } from "../../lib/conversationView";
import type { RoomMemberDto, RoomMessageDto } from "../../lib/types";
import { RoomComposer } from "./RoomComposer";
import { RoomMessage } from "./RoomMessage";
import { RoomThread } from "./RoomThread";

const members: RoomMemberDto[] = [
  {
    conversation_id: "c1",
    agent_name: "researcher",
    agent_url: "http://r",
    joined_at: "2026-09-13T00:00:00+00:00",
  },
  {
    conversation_id: "c1",
    agent_name: "writer",
    agent_url: "http://w",
    joined_at: "2026-09-13T00:00:00+00:00",
  },
];

function message(overrides: Partial<RoomMessageDto> = {}): RoomMessageDto {
  return {
    id: "m1",
    conversation_id: "c1",
    seq: 1,
    role: "user",
    sender: "CEO",
    text: "大家好",
    mentions: [],
    created_at: "2026-09-13T00:00:00+00:00",
    ...overrides,
  };
}

describe("RoomMessage", () => {
  it("renders user, agent and assistant messages with mentions highlighted", () => {
    const { rerender } = render(
      <RoomMessage
        message={message()}
        members={members}
        onQuote={() => {}}
        onInterrupt={() => {}}
      />,
    );
    expect(screen.getByText("CEO")).toBeInTheDocument();

    rerender(
      <RoomMessage
        message={message({
          id: "m2",
          role: "agent",
          sender: "researcher",
          text: "@writer 请补充数据",
          mentions: ["writer"],
        })}
        members={members}
        onQuote={() => {}}
        onInterrupt={() => {}}
      />,
    );
    expect(screen.getByText("researcher")).toBeInTheDocument();
    expect(screen.getByText("@writer")).toHaveClass("mention");
  });

  it("shows the quote block and queued badge", () => {
    render(
      <RoomMessage
        message={message({
          id: "m3",
          role: "user",
          text: "补充一点",
          quote_id: "m1",
          queued_for_node_id: "p1:n1",
        })}
        members={members}
        quote={message({ id: "m1", text: "原问题" })}
        onQuote={() => {}}
        onInterrupt={() => {}}
      />,
    );
    expect(screen.getByText("排队中")).toBeInTheDocument();
    expect(screen.getByText("原问题")).toBeInTheDocument();
  });

  it("offers interrupt for a working message", async () => {
    const user = userEvent.setup();
    const onInterrupt = vi.fn();
    render(
      <RoomMessage
        message={message({
          id: "m4",
          role: "agent",
          sender: "researcher",
          node_id: "p1:n1",
          text: "处理中",
        })}
        members={members}
        working
        onQuote={() => {}}
        onInterrupt={onInterrupt}
      />,
    );
    await user.click(screen.getByRole("button", { name: "打断" }));
    expect(onInterrupt).toHaveBeenCalled();
  });
});

describe("RoomThread", () => {
  it("renders persisted messages and transient working bubbles", () => {
    render(
      <RoomThread
        view={{ ...emptyConversation, messages: [message()], members }}
        workingBubbles={[
          { nodeId: "p1:n1", agentName: "researcher", text: "正在生成" },
        ]}
        onQuote={() => {}}
        onInterrupt={() => {}}
      />,
    );
    expect(screen.getByText("大家好")).toBeInTheDocument();
    expect(screen.getByText("正在生成")).toBeInTheDocument();
    expect(screen.getByText("正在输入…")).toBeInTheDocument();
  });
});

describe("RoomComposer", () => {
  it("autocompletes mentions with the keyboard", async () => {
    const user = userEvent.setup();
    const onSend = vi.fn().mockResolvedValue(undefined);
    render(
      <RoomComposer
        members={members}
        replyTo={null}
        interrupt={false}
        onToggleInterrupt={() => {}}
        onCancelReply={() => {}}
        onSend={onSend}
      />,
    );
    const textarea = screen.getByPlaceholderText("输入消息，@ 指派 Agent，Enter 发送");
    await user.type(textarea, "@res");
    expect(screen.getByRole("button", { name: "@researcher" })).toBeInTheDocument();
    await user.keyboard("{Enter}");
    expect((textarea as HTMLTextAreaElement).value).toBe("@researcher ");
    await user.type(textarea, "请调研");
    await user.keyboard("{Enter}");
    expect(onSend).toHaveBeenCalledWith({
      text: "@researcher 请调研",
      mentions: ["researcher"],
    });
  });

  it("sends quote and interrupt flags", async () => {
    const user = userEvent.setup();
    const onSend = vi.fn().mockResolvedValue(undefined);
    const onToggleInterrupt = vi.fn();
    render(
      <RoomComposer
        members={members}
        replyTo={message({
          id: "m9",
          role: "agent",
          sender: "researcher",
          node_id: "p1:n1",
          text: "长任务",
        })}
        interrupt
        onToggleInterrupt={onToggleInterrupt}
        onCancelReply={() => {}}
        onSend={onSend}
      />,
    );
    expect(screen.getByText(/引用 researcher/)).toBeInTheDocument();
    const checkbox = screen.getByRole("checkbox");
    expect(checkbox).toBeChecked();
    await user.click(checkbox);
    expect(onToggleInterrupt).toHaveBeenCalledWith(false);

    await user.type(screen.getByPlaceholderText("输入消息，@ 指派 Agent，Enter 发送"), "改需求");
    await user.keyboard("{Enter}");
    expect(onSend).toHaveBeenCalledWith({
      text: "改需求",
      mentions: [],
      quote_id: "m9",
      interrupt: true,
    });
  });
});
