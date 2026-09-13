import type { RoomView } from "../../lib/roomView";
import type { RoomMessageDto } from "../../lib/types";
import { RoomMessage } from "./RoomMessage";

export interface WorkingBubble {
  nodeId: string;
  agentName: string;
  text: string;
}

export function RoomThread({
  view,
  workingBubbles,
  onQuote,
  onInterrupt,
}: {
  view: RoomView;
  workingBubbles: WorkingBubble[];
  onQuote: (message: RoomMessageDto) => void;
  onInterrupt: (message: RoomMessageDto) => void;
}) {
  const byId = new Map(view.messages.map((message) => [message.id, message]));
  const hasContent = view.messages.length > 0 || workingBubbles.length > 0;

  return (
    <div className="room-thread">
      {!hasContent ? (
        <div className="thread-empty">
          <h2>开始协作</h2>
          <p>
            描述你的目标，assistant 会拆解任务并把 Agent 拉进群里；用 @ 指派、引用回复，
            需要时随时插话。
          </p>
        </div>
      ) : null}
      {view.messages.map((message) => (
        <RoomMessage
          key={message.id}
          message={message}
          members={view.members}
          quote={message.quote_id ? (byId.get(message.quote_id) ?? null) : null}
          onQuote={onQuote}
          onInterrupt={onInterrupt}
        />
      ))}
      {workingBubbles.map((bubble) => (
        <RoomMessage
          key={`working-${bubble.nodeId}`}
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
      ))}
    </div>
  );
}
