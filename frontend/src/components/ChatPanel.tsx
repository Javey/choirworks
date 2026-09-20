import { useEffect, useRef, useState } from "react";
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

function ThinkingSection({ text }: { text: string }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div className="mb-1.5">
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        className="flex items-center gap-1 text-[11px] text-feishu-muted hover:text-feishu-text transition-colors"
      >
        <span className="text-[10px]">{expanded ? "▾" : "▸"}</span>
        思考
      </button>
      {expanded ? (
        <div className="mt-1 px-3 py-2 rounded-lg bg-feishu-bg text-feishu-text-secondary text-sm prose-sm max-w-none border border-feishu-border-light">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>
            {text}
          </ReactMarkdown>
        </div>
      ) : null}
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

function AgentBubble({ msg, thinking }: { msg: ChatMessage; thinking?: ChatMessage | null }) {
  const sender = msg.sender || "agent";
  return (
    <div className="flex gap-2.5">
      <Avatar name={sender} />
      <div className="flex-1 max-w-[70%]">
        {thinking ? <ThinkingSection text={thinking.text} /> : null}
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

function AssistantTextBubble({ msg, thinking }: { msg: ChatMessage; thinking?: ChatMessage | null }) {
  return (
    <div className="flex gap-2.5">
      <Avatar name="规划大脑" />
      <div className="flex-1 max-w-[70%]">
        {thinking ? <ThinkingSection text={thinking.text} /> : null}
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tl-md bg-feishu-primary-soft border border-feishu-primary-border text-feishu-text text-sm prose-sm max-w-none">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>
            {msg.text}
          </ReactMarkdown>
        </div>
      </div>
    </div>
  );
}

function NotificationItem({ notif, thinking }: { notif: SystemNotification; thinking?: ChatMessage | null }) {
  return (
    <div className="flex flex-col items-center gap-1">
      {thinking ? (
        <div className="w-full max-w-[70%]">
          <ThinkingSection text={thinking.text} />
        </div>
      ) : null}
      <div className="text-[12px] text-feishu-muted bg-feishu-bg px-3 py-1 rounded-full">
        {notif.text}
      </div>
    </div>
  );
}

function WorkingItem({ bubble, thinking }: { bubble: WorkingBubble; thinking?: ChatMessage | null }) {
  const sender = bubble.agentName || "agent";
  return (
    <div className="flex gap-2.5">
      <Avatar name={sender} />
      <div className="flex-1 max-w-[70%]">
        {thinking ? <ThinkingSection text={thinking.text} /> : null}
        <div className="text-[11px] text-feishu-muted mb-1 ml-1">
          @{sender} 正在工作
        </div>
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tl-md bg-white border border-feishu-border text-feishu-text text-sm">
          <span className="whitespace-pre-wrap break-words">{bubble.text}</span>
          <span className="caret" />
        </div>
      </div>
    </div>
  );
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

  const rendered: React.ReactNode[] = [];
  let pendingThinking: ChatMessage | null = null;

  for (const item of items) {
    if (item.type === "message" && item.data.thinking) {
      pendingThinking = item.data;
      continue;
    }

    if (item.type === "message") {
      if (item.data.role === "user") {
        if (pendingThinking) {
          rendered.push(
            <div key={`msg-${item.data.id}`} className="animate-[fade-in-up_0.2s_ease-out]">
              <UserBubble msg={item.data} />
            </div>,
          );
          pendingThinking = null;
        } else {
          rendered.push(
            <div key={`msg-${item.data.id}`} className="animate-[fade-in-up_0.2s_ease-out]">
              <UserBubble msg={item.data} />
            </div>,
          );
        }
      } else {
        const bubble = item.data.role === "assistant"
          ? <AssistantTextBubble msg={item.data} thinking={pendingThinking} />
          : <AgentBubble msg={item.data} thinking={pendingThinking} />;
        rendered.push(
          <div key={`msg-${item.data.id}`} className="animate-[fade-in-up_0.2s_ease-out]">
            {bubble}
          </div>,
        );
        pendingThinking = null;
      }
    } else if (item.type === "notification") {
      rendered.push(
        <div key={`notif-${item.data.id}`} className="animate-[fade-in_0.3s_ease-out]">
          <NotificationItem notif={item.data} thinking={pendingThinking} />
        </div>,
      );
      pendingThinking = null;
    } else {
      rendered.push(
        <div key={`working-${item.data.artifactId}`} className="animate-[fade-in_0.3s_ease-out]">
          <WorkingItem bubble={item.data} thinking={pendingThinking} />
        </div>,
      );
      pendingThinking = null;
    }
  }

  if (pendingThinking) {
    rendered.push(
      <div key="pending-thinking" className="animate-[fade-in_0.3s_ease-out]">
        <div className="flex gap-2.5">
          <Avatar name="规划大脑" />
          <div className="flex-1 max-w-[70%]">
            <ThinkingSection text={pendingThinking.text} />
          </div>
        </div>
      </div>,
    );
  }

  return (
    <div className="flex-1 min-h-0 overflow-y-auto bg-feishu-bg/50 px-4 py-4">
      <div className="flex flex-col gap-3 max-w-3xl mx-auto">
        {rendered}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
