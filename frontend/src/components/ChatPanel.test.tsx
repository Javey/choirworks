import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ChatPanel } from "./ChatPanel";
import { emptyConversation, type ChatMessage, type ConversationView } from "../lib/conversationView";

function msg(partial: Partial<ChatMessage> & { id: string; text: string }): ChatMessage {
  return {
    role: "assistant",
    sender: null,
    mentions: [],
    quote_id: null,
    node_id: null,
    task_id: null,
    created_at: "",
    seq: 0,
    thinking: false,
    ...partial,
  };
}

describe("ChatPanel thinking", () => {
  it("renders a trailing thinking item with the assistant avatar", () => {
    render(
      <ChatPanel
        view={{ ...emptyConversation, messages: [msg({ id: "t1", text: "推理内容", thinking: true, seq: 1 })] }}
      />,
    );

    expect(screen.getByText("思考")).toBeTruthy();
    expect(screen.getByText("规")).toBeTruthy();

    fireEvent.click(screen.getByText("思考"));
    expect(screen.getByText("推理内容")).toBeTruthy();
  });

  it("renders consecutive thinking blocks as separate collapsed divs, before notifications", () => {
    const view: ConversationView = {
      ...emptyConversation,
      messages: [
        msg({ id: "t1", text: "第一段思考", thinking: true, seq: 1 }),
        msg({ id: "t2", text: "第二段思考", thinking: true, seq: 2 }),
      ],
      notifications: [{
        id: "n1",
        kind: "room.participant_joined",
        text: "x 加入了群聊",
        created_at: "",
        seq: 3,
      }],
    };
    render(<ChatPanel view={view} />);

    const buttons = screen.getAllByText("思考");
    expect(buttons).toHaveLength(2);
    expect(screen.getAllByText("规")).toHaveLength(1);

    fireEvent.click(buttons[0]);
    expect(screen.getByText("第一段思考")).toBeTruthy();
    expect(screen.queryByText("第二段思考")).toBeNull();

    fireEvent.click(buttons[1]);
    expect(screen.getByText("第二段思考")).toBeTruthy();
    expect(screen.getByText("x 加入了群聊")).toBeTruthy();
  });

  it("shares one avatar between thinking blocks and the assistant reply they precede", () => {
    const view: ConversationView = {
      ...emptyConversation,
      messages: [
        msg({ id: "t1", text: "先想一下", thinking: true, seq: 1 }),
        msg({ id: "t2", text: "再确认一下", thinking: true, seq: 2 }),
        msg({ id: "a1", text: "答复正文", seq: 3 }),
      ],
    };
    render(<ChatPanel view={view} />);

    expect(screen.getAllByText("规")).toHaveLength(1);
    expect(screen.getAllByText("思考")).toHaveLength(2);
    fireEvent.click(screen.getAllByText("思考")[0]);
    fireEvent.click(screen.getAllByText("思考")[1]);
    expect(screen.getByText("先想一下")).toBeTruthy();
    expect(screen.getByText("再确认一下")).toBeTruthy();
    expect(screen.getByText("答复正文")).toBeTruthy();
  });
});

describe("ChatPanel avatar grouping", () => {
  it("shows one avatar for consecutive agent messages from the same sender", () => {
    const view: ConversationView = {
      ...emptyConversation,
      messages: [
        msg({ id: "m1", role: "agent", sender: "writer", text: "第一段", seq: 1 }),
        msg({ id: "m2", role: "agent", sender: "writer", text: "第二段", seq: 2 }),
      ],
    };
    render(<ChatPanel view={view} />);

    expect(screen.getAllByText("W")).toHaveLength(1);
    expect(screen.getAllByText("@writer")).toHaveLength(1);
    expect(screen.getByText("第一段")).toBeTruthy();
    expect(screen.getByText("第二段")).toBeTruthy();
  });

  it("shows separate avatars for consecutive agent messages from different senders", () => {
    const view: ConversationView = {
      ...emptyConversation,
      messages: [
        msg({ id: "m1", role: "agent", sender: "writer", text: "来自 writer", seq: 1 }),
        msg({ id: "m2", role: "agent", sender: "reviewer", text: "来自 reviewer", seq: 2 }),
      ],
    };
    render(<ChatPanel view={view} />);

    expect(screen.getAllByText("W")).toHaveLength(1);
    expect(screen.getAllByText("R")).toHaveLength(1);
    expect(screen.getAllByText("@writer")).toHaveLength(1);
    expect(screen.getAllByText("@reviewer")).toHaveLength(1);
  });

  it("resets avatar grouping when a notification interrupts", () => {
    const view: ConversationView = {
      ...emptyConversation,
      messages: [
        msg({ id: "m1", role: "agent", sender: "writer", text: "第一段", seq: 1 }),
        msg({ id: "m2", role: "agent", sender: "writer", text: "第二段", seq: 3 }),
      ],
      notifications: [{
        id: "n1",
        kind: "room.participant_joined",
        text: "reviewer 加入了群聊",
        created_at: "",
        seq: 2,
      }],
    };
    render(<ChatPanel view={view} />);

    expect(screen.getAllByText("W")).toHaveLength(2);
    expect(screen.getAllByText("@writer")).toHaveLength(2);
  });

  it("shows one avatar for consecutive assistant messages", () => {
    const view: ConversationView = {
      ...emptyConversation,
      messages: [
        msg({ id: "m1", role: "assistant", text: "第一条", seq: 1 }),
        msg({ id: "m2", role: "assistant", text: "第二条", seq: 2 }),
      ],
    };
    render(<ChatPanel view={view} />);

    expect(screen.getAllByText("规")).toHaveLength(1);
    expect(screen.getByText("第一条")).toBeTruthy();
    expect(screen.getByText("第二条")).toBeTruthy();
  });

  it("shows separate avatars for agent then assistant", () => {
    const view: ConversationView = {
      ...emptyConversation,
      messages: [
        msg({ id: "m1", role: "agent", sender: "writer", text: "agent 回复", seq: 1 }),
        msg({ id: "m2", role: "assistant", text: "assistant 回复", seq: 2 }),
      ],
    };
    render(<ChatPanel view={view} />);

    expect(screen.getAllByText("W")).toHaveLength(1);
    expect(screen.getAllByText("规")).toHaveLength(1);
  });

  it("resets grouping after a user message", () => {
    const view: ConversationView = {
      ...emptyConversation,
      messages: [
        msg({ id: "m1", role: "agent", sender: "writer", text: "第一段", seq: 1 }),
        msg({ id: "m2", role: "user", text: "用户追问", seq: 2 }),
        msg({ id: "m3", role: "agent", sender: "writer", text: "第二段", seq: 3 }),
      ],
    };
    render(<ChatPanel view={view} />);

    expect(screen.getAllByText("W")).toHaveLength(2);
    expect(screen.getAllByText("@writer")).toHaveLength(2);
  });
});
