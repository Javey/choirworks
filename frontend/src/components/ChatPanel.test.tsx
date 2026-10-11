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
