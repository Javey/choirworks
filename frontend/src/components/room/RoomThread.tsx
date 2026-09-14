import { Sparkles } from "lucide-react";

import type { RoomMemberDto, RoomMessageDto } from "../../lib/types";
import { groupMessages } from "../../lib/messageGroup";
import { TimeDivider } from "../TimeDivider";
import { RoomMessage } from "./RoomMessage";

export interface WorkingBubble {
  nodeId: string;
  agentName: string;
  text: string;
}

interface SimpleRoomView {
  messages: RoomMessageDto[];
  members: RoomMemberDto[];
  summary?: unknown;
  lastSeq: number;
}

export function RoomThread({
  view,
  workingBubbles,
  onQuote,
  onInterrupt,
}: {
  view: SimpleRoomView;
  workingBubbles: WorkingBubble[];
  onQuote: (message: RoomMessageDto) => void;
  onInterrupt: (message: RoomMessageDto) => void;
}) {
  const byId = new Map(view.messages.map((message) => [message.id, message]));
  const hasContent = view.messages.length > 0 || workingBubbles.length > 0;
  const items = groupMessages(view.messages);

  return (
    <div className="flex-1 min-h-0 overflow-y-auto px-6 py-4">
      <div className="flex flex-col gap-1 max-w-[880px] w-full mx-auto">
        {!hasContent ? (
          <div className="flex flex-col items-center justify-center py-20 text-center">
            <div className="flex items-center justify-center w-16 h-16 rounded-full bg-feishu-primary-soft mb-4">
              <Sparkles size={28} className="text-feishu-primary" />
            </div>
            <h2 className="text-lg font-bold text-feishu-text mb-2">
              开始协作
            </h2>
            <p className="text-sm text-feishu-muted max-w-xs leading-relaxed">
              描述你的目标，assistant 会拆解任务并把 Agent 拉进群里；用 @ 指派、引用回复，需要时随时插话。
            </p>
          </div>
        ) : null}

        {items.map((item) => {
          if (item.kind === "divider") {
            return <TimeDivider key={item.key} text={item.text} />;
          }
          const message = item.message;
          return (
            <div
              key={item.key}
              className={item.grouped ? "mt-0.5" : "mt-2"}
              style={{ animation: "fade-in-up 0.2s ease-out" }}
            >
              <RoomMessage
                message={message}
                members={view.members}
                quote={message.quote_id ? (byId.get(message.quote_id) ?? null) : null}
                grouped={item.grouped}
                showTime={item.showTime}
                onQuote={onQuote}
                onInterrupt={onInterrupt}
              />
            </div>
          );
        })}

        {workingBubbles.map((bubble) => (
          <div
            key={`working-${bubble.nodeId}`}
            className="mt-2"
            style={{ animation: "fade-in-up 0.2s ease-out" }}
          >
            <RoomMessage
              message={{
                id: `working-${bubble.nodeId}`,
                conversation_id: "",
                seq: Number.MAX_SAFE_INTEGER,
                role: "agent",
                sender: bubble.agentName,
                text: "",
                mentions: [],
                node_id: bubble.nodeId,
                created_at: "",
              }}
              members={view.members}
              working
              streamingText={bubble.text}
              onQuote={onQuote}
              onInterrupt={onInterrupt}
            />
          </div>
        ))}
      </div>
    </div>
  );
}
