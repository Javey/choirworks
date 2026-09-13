import type { ReactNode } from "react";

import type { RoomMemberDto, RoomMessageDto } from "../../lib/types";

export function QuoteBlock({ quote }: { quote: RoomMessageDto }) {
  return (
    <a className="room-quote" href={`#msg-${quote.id}`}>
      <span className="room-quote-sender">
        {quote.sender ?? quote.role}
      </span>
      <span className="room-quote-text">{quote.text.slice(0, 60)}</span>
    </a>
  );
}

export function highlightMentions(
  text: string,
  memberNames: Set<string>,
): ReactNode[] {
  const parts = text.split(/(@[A-Za-z0-9_-]+)/g);
  return parts.map((part, index) => {
    const name = part.startsWith("@") ? part.slice(1) : null;
    if (name && memberNames.has(name)) {
      return (
        <span key={index} className="mention">
          {part}
        </span>
      );
    }
    return <span key={index}>{part}</span>;
  });
}

const ROLE_LABEL: Record<RoomMessageDto["role"], string> = {
  user: "CEO",
  assistant: "assistant",
  agent: "",
  system: "系统",
};

export function RoomMessage({
  message,
  members,
  quote,
  working = false,
  streamingText,
  onQuote,
  onInterrupt,
}: {
  message: RoomMessageDto;
  members: RoomMemberDto[];
  quote?: RoomMessageDto | null;
  working?: boolean;
  streamingText?: string;
  onQuote: (message: RoomMessageDto) => void;
  onInterrupt: (message: RoomMessageDto) => void;
}) {
  const label =
    message.role === "agent"
      ? (message.sender ?? "agent")
      : ROLE_LABEL[message.role];
  const memberNames = new Set(members.map((member) => member.agent_name));
  const displayText = streamingText ?? message.text;
  const queued = Boolean(message.queued_for_node_id && !message.delivered_at);

  return (
    <div
      className={`room-msg ${message.role}`}
      id={`msg-${message.id}`}
      data-seq={message.seq}
    >
      {message.role === "agent" || message.role === "assistant" ? (
        <div className={`room-avatar ${message.role}`}>
          {label.slice(0, 1).toUpperCase()}
        </div>
      ) : null}
      <div className="room-bubble">
        <div className="room-meta">
          <span className="room-sender">{label}</span>
          {queued ? <span className="badge warn">排队中</span> : null}
          {working ? <span className="badge">正在输入…</span> : null}
          <span className="room-actions">
            <button
              type="button"
              className="room-action"
              onClick={() => onQuote(message)}
            >
              引用
            </button>
            {working && message.node_id ? (
              <button
                type="button"
                className="room-action danger"
                onClick={() => onInterrupt(message)}
              >
                打断
              </button>
            ) : null}
          </span>
        </div>
        {quote ? <QuoteBlock quote={quote} /> : null}
        <div className="room-text">
          {highlightMentions(displayText, memberNames)}
          {working ? <span className="room-caret" /> : null}
        </div>
      </div>
    </div>
  );
}
