import { useEffect, useRef } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import type {
  ChatMessage,
  ConversationView,
  SystemNotification,
  WorkingBubble,
} from "../lib/conversationView";

type TimelineItem =
  | { type: "message"; seq: number; data: ChatMessage }
  | { type: "notification"; seq: number; data: SystemNotification }
  | { type: "working"; seq: number; data: WorkingBubble };

const AVATAR_COLORS = [
  "#3370ff", "#7c3aed", "#34c759", "#ff9500",
  "#f54a45", "#00a6fb", "#e056fd", "#fa7268",
];

function avatarColor(name: string): string {
  const hash = name.split("").reduce((acc, ch) => acc + ch.charCodeAt(0), 0);
  return AVATAR_COLORS[hash % AVATAR_COLORS.length];
}

function Avatar({ name, size = 36 }: { name: string; size?: number }) {
  const color = avatarColor(name);
  return (
    <div
      className="flex items-center justify-center rounded-full text-white font-semibold shrink-0"
      style={{ background: color, width: size, height: size, fontSize: size * 0.4 }}
    >
      {name.slice(0, 1).toUpperCase()}
    </div>
  );
}

function UserBubble({ msg }: { msg: ChatMessage }) {
  return (
    <div className="flex justify-end gap-2.5">
      <div className="flex-1 flex flex-col items-end max-w-[70%]">
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tr-md bg-feishu-primary text-white text-sm whitespace-pre-wrap break-words">
          {msg.text}
        </div>
      </div>
      <Avatar name="我" />
    </div>
  );
}

function AssistantBubble({ msg }: { msg: ChatMessage }) {
  const sender = msg.sender || "规划大脑";
  return (
    <div className="flex gap-2.5">
      <Avatar name={sender} />
      <div className="flex-1 max-w-[70%]">
        <div className="text-[11px] text-feishu-muted mb-1 ml-1">{sender}</div>
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tl-md bg-feishu-agent-soft border border-feishu-primary-border text-feishu-text text-sm prose-sm max-w-none">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>
            {msg.text}
          </ReactMarkdown>
        </div>
      </div>
    </div>
  );
}

function AgentBubble({ msg }: { msg: ChatMessage }) {
  const sender = msg.sender || "agent";
  return (
    <div className="flex gap-2.5">
      <Avatar name={sender} />
      <div className="flex-1 max-w-[70%]">
        <div className="text-[11px] text-feishu-muted mb-1 ml-1">@{sender}</div>
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tl-md bg-white border border-feishu-border text-feishu-text text-sm prose-sm max-w-none">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>
            {msg.text}
          </ReactMarkdown>
        </div>
      </div>
    </div>
  );
}

function NotificationItem({ notif }: { notif: SystemNotification }) {
  return (
    <div className="flex justify-center">
      <div className="text-[12px] text-feishu-muted bg-feishu-bg px-3 py-1 rounded-full">
        {notif.text}
      </div>
    </div>
  );
}

function WorkingItem({ bubble }: { bubble: WorkingBubble }) {
  return (
    <div className="flex gap-2.5">
      <Avatar name={bubble.agentName || "agent"} />
      <div className="flex-1 max-w-[70%]">
        <div className="text-[11px] text-feishu-muted mb-1 ml-1">
          {bubble.agentName ? `@${bubble.agentName}` : "agent"} 正在工作
        </div>
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tl-md bg-white border border-feishu-border text-feishu-text text-sm">
          <span className="whitespace-pre-wrap break-words">{bubble.text}</span>
          <span className="caret" />
        </div>
      </div>
    </div>
  );
}

function MessageBubble({ msg }: { msg: ChatMessage }) {
  if (msg.role === "user") return <UserBubble msg={msg} />;
  if (msg.role === "assistant") return <AssistantBubble msg={msg} />;
  return <AgentBubble msg={msg} />;
}

export function ChatPanel({ view }: { view: ConversationView }) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [view.messages.length, view.notifications.length, view.workingBubbles.length]);

  const items: TimelineItem[] = [
    ...view.messages.map((m) => ({ type: "message" as const, seq: m.seq, data: m })),
    ...view.notifications.map((n) => ({ type: "notification" as const, seq: n.seq, data: n })),
    ...view.workingBubbles.map((b, i) => ({ type: "working" as const, seq: 1000000 + i, data: b })),
  ].sort((a, b) => a.seq - b.seq);

  if (items.length === 0) {
    return (
      <div className="flex-1 min-h-0 flex items-center justify-center bg-feishu-bg/50">
        <div className="text-sm text-feishu-muted">输入消息开始对话</div>
      </div>
    );
  }

  return (
    <div className="flex-1 min-h-0 overflow-y-auto bg-feishu-bg/50 px-4 py-4">
      <div className="flex flex-col gap-3 max-w-3xl mx-auto">
        {items.map((item) => {
          if (item.type === "message") {
            return (
              <div key={`msg-${item.data.id}`} className="animate-[fade-in-up_0.2s_ease-out]">
                <MessageBubble msg={item.data} />
              </div>
            );
          }
          if (item.type === "notification") {
            return (
              <div key={`notif-${item.data.id}`} className="animate-[fade-in_0.3s_ease-out]">
                <NotificationItem notif={item.data} />
              </div>
            );
          }
          return (
            <div key={`working-${item.data.nodeId}`} className="animate-[fade-in_0.3s_ease-out]">
              <WorkingItem bubble={item.data} />
            </div>
          );
        })}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
