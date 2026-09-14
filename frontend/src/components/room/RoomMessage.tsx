import { Reply, Square } from "lucide-react";

import type { ReactNode } from "react";

import type { RoomMemberDto, RoomMessageDto } from "../../lib/types";
import { formatTime } from "../TimeDivider";
import { Avatar } from "../Avatar";
import { Markdown } from "../Markdown";
import { highlightMentions } from "../Markdown";

export function QuoteBlock({ quote }: { quote: RoomMessageDto }) {
  return (
    <a
      href={`#msg-${quote.id}`}
      className="block border-l-[3px] border-feishu-primary bg-feishu-primary-soft/50 rounded-md px-2.5 py-1.5 my-1 text-xs text-feishu-text-secondary no-underline hover:bg-feishu-primary-soft transition-colors"
    >
      <span className="font-semibold mr-1.5 text-feishu-text">
        {quote.sender ?? quote.role}
      </span>
      <span>{quote.text.slice(0, 60)}</span>
    </a>
  );
}

const ROLE_LABEL: Record<RoomMessageDto["role"], string> = {
  user: "CEO",
  assistant: "assistant",
  agent: "",
  system: "系统",
};

const AVATAR_BG: Record<string, string> = {
  agent: "#7c3aed",
  assistant: "#3370ff",
  user: "#3370ff",
  system: "#8f959e",
};

export function RoomMessage({
  message,
  members,
  quote,
  working = false,
  streamingText,
  grouped = false,
  showTime = true,
  onQuote,
  onInterrupt,
}: {
  message: RoomMessageDto;
  members: RoomMemberDto[];
  quote?: RoomMessageDto | null;
  working?: boolean;
  streamingText?: string;
  grouped?: boolean;
  showTime?: boolean;
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
  const isUser = message.role === "user";
  const isSystem = message.role === "system";
  const isAgent = message.role === "agent" || message.role === "assistant";

  if (isSystem) {
    return (
      <div className="flex justify-center py-1" id={`msg-${message.id}`}>
        <span className="px-3 py-1 rounded-full bg-feishu-border-light text-xs text-feishu-muted">
          {displayText}
        </span>
      </div>
    );
  }

  const showAvatar = !grouped;
  const showHeader = !grouped;
  const time = message.created_at ? formatTime(message.created_at) : "";

  return (
    <div
      className={`flex gap-2.5 items-start group ${isUser ? "flex-row-reverse" : ""}`}
      id={`msg-${message.id}`}
      data-seq={message.seq}
    >
      {showAvatar && isAgent ? (
        <Avatar name={label} size={36} bg={AVATAR_BG[message.role]} />
      ) : showAvatar ? (
        <Avatar name={label} size={36} bg={AVATAR_BG[message.role]} />
      ) : (
        <div className="w-9 shrink-0" />
      )}

      <div className={`flex flex-col min-w-0 max-w-[75%] ${isUser ? "items-end" : "items-start"}`}>
        {showHeader && (
          <div className={`flex items-center gap-2 mb-1 ${isUser ? "flex-row-reverse" : ""}`}>
            <span className="text-[13px] font-semibold text-feishu-text">
              {label}
            </span>
            {showTime && time ? (
              <span className="text-[11px] text-feishu-muted">{time}</span>
            ) : null}
            {queued ? (
              <span className="px-1.5 py-0.5 rounded-full bg-feishu-warn-soft text-[10px] text-feishu-warn border border-feishu-warn/20">
                排队中
              </span>
            ) : null}
            {working ? (
              <span className="px-1.5 py-0.5 rounded-full bg-feishu-primary-soft text-[10px] text-feishu-primary border border-feishu-primary/20">
                正在输入…
              </span>
            ) : null}
          </div>
        )}

        <div className="relative">
          <div
            className={`inline-block px-3 py-2 rounded-[12px] text-[14px] leading-relaxed break-words ${
              isUser
                ? "bg-feishu-primary text-white rounded-br-[4px]"
                : message.role === "assistant"
                  ? "bg-feishu-primary-soft text-feishu-text border border-feishu-primary-border rounded-bl-[4px]"
                  : "bg-white text-feishu-text border border-feishu-border rounded-bl-[4px]"
            }`}
          >
            {quote ? <QuoteBlock quote={quote} /> : null}
            <div className="whitespace-pre-wrap break-words">
              {isUser ? (
                <>
                  {highlightMentions(displayText, memberNames)}
                  {working ? <span className="caret" /> : null}
                </>
              ) : (
                <>
                  <Markdown memberNames={memberNames}>{displayText}</Markdown>
                  {working ? <span className="caret" /> : null}
                </>
              )}
            </div>
          </div>

          <div
            className={`absolute top-0 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity ${
              isUser ? "right-full mr-1" : "left-full ml-1"
            }`}
          >
            <button
              type="button"
              title="引用"
              onClick={() => onQuote(message)}
              className="flex items-center justify-center w-7 h-7 rounded-lg bg-white border border-feishu-border text-feishu-muted hover:text-feishu-primary hover:border-feishu-primary transition-colors"
            >
              <Reply size={14} />
            </button>
            {working && message.node_id ? (
              <button
                type="button"
                title="打断"
                onClick={() => onInterrupt(message)}
                className="flex items-center justify-center w-7 h-7 rounded-lg bg-white border border-feishu-border text-feishu-danger hover:bg-feishu-danger-soft transition-colors"
              >
                <Square size={12} />
              </button>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}

export type { ReactNode };
