import { useEffect, useRef, useState, type ComponentProps } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import type {
  ChatMessage,
  ConversationView,
  QuestionInfo,
  SystemNotification,
  WorkingBubble,
} from "../lib/conversationView";
import { mergeConsecutiveJoins, type TimelineItem } from "../lib/conversationView";
import { isMemberAnchor, remarkMention, splitMentions } from "../lib/mentions";
import { QuestionCard } from "./room/QuestionCard";

const AVATAR_COLORS = [
  "#3370ff", "#7c3aed", "#34c759", "#ff9500",
  "#f54a45", "#00a6fb", "#e056fd", "#fa7268",
];

function avatarColor(name: string): string {
  const hash = name.split("").reduce((acc, ch) => acc + ch.charCodeAt(0), 0);
  return AVATAR_COLORS[hash % AVATAR_COLORS.length];
}

function Avatar({ name, size = 36, hidden = false }: { name: string; size?: number; hidden?: boolean }) {
  if (hidden) {
    return <div className="shrink-0" style={{ width: size, height: size }} />;
  }
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

function MentionSpan({ name, onPrimary }: { name: string; onPrimary?: boolean }) {
  return (
    <span
      className={
        onPrimary
          ? "bg-white/25 text-white rounded px-0.5 font-medium"
          : "text-feishu-primary font-medium"
      }
    >
      {name}
    </span>
  );
}

function MentionText({ text, onPrimary }: { text: string; onPrimary?: boolean }) {
  return (
    <>
      {splitMentions(text).map((segment, i) =>
        segment.kind === "mention" ? (
          <MentionSpan key={i} name={segment.value} onPrimary={onPrimary} />
        ) : (
          <span key={i}>{segment.value}</span>
        ),
      )}
    </>
  );
}

function MentionAnchor({ href, children }: ComponentProps<"a">) {
  if (isMemberAnchor(href)) {
    return <span className="text-feishu-primary font-medium">{children}</span>;
  }
  return <a href={href}>{children}</a>;
}

function UserBubble({ msg }: { msg: ChatMessage }) {
  return (
    <div className="flex justify-end gap-2.5">
      <div className="flex-1 flex flex-col items-end max-w-[70%]">
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tr-md bg-feishu-primary text-white text-sm whitespace-pre-wrap break-words">
          <MentionText text={msg.text} onPrimary />
        </div>
      </div>
      <Avatar name="我" />
    </div>
  );
}

function AgentBubble({ msg, showAvatar = true }: { msg: ChatMessage; showAvatar?: boolean }) {
  const sender = msg.sender || "agent";
  return (
    <div className="flex gap-2.5">
      <Avatar name={sender} hidden={!showAvatar} />
      <div className="flex-1 max-w-[70%]">
        {showAvatar ? <div className="text-[11px] text-feishu-muted mb-1 ml-1">@{sender}</div> : null}
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tl-md bg-white border border-feishu-border text-feishu-text text-sm prose-sm max-w-none">
          <ReactMarkdown
            remarkPlugins={[remarkGfm, remarkMention]}
            components={{ a: MentionAnchor }}
          >
            {msg.text}
          </ReactMarkdown>
        </div>
      </div>
    </div>
  );
}

function AssistantTextBubble({ msg, thinking, showAvatar = true }: { msg: ChatMessage; thinking?: string[] | null; showAvatar?: boolean }) {
  return (
    <div className="flex gap-2.5">
      <Avatar name="规划大脑" hidden={!showAvatar} />
      <div className="flex-1 max-w-[70%]">
        {thinking?.map((text, index) => <ThinkingSection key={index} text={text} />)}
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tl-md bg-feishu-primary-soft border border-feishu-primary-border text-feishu-text text-sm prose-sm max-w-none">
          <ReactMarkdown
            remarkPlugins={[remarkGfm, remarkMention]}
            components={{ a: MentionAnchor }}
          >
            {msg.text}
          </ReactMarkdown>
        </div>
      </div>
    </div>
  );
}

function ThinkingItem({ blocks, showAvatar = true }: { blocks: string[]; showAvatar?: boolean }) {
  return (
    <div className="flex gap-2.5">
      <Avatar name="规划大脑" hidden={!showAvatar} />
      <div className="flex-1 max-w-[70%]">
        {blocks.map((text, index) => <ThinkingSection key={index} text={text} />)}
      </div>
    </div>
  );
}

function NotificationItem({ notif }: { notif: SystemNotification }) {
  return (
    <div className="flex flex-col items-center gap-1">
      <div className="text-[12px] text-feishu-muted bg-feishu-bg px-3 py-1 rounded-full">
        {notif.text}
      </div>
    </div>
  );
}

function WorkingItem({ bubble, showAvatar = true }: { bubble: WorkingBubble; showAvatar?: boolean }) {
  const sender = bubble.author || "agent";
  return (
    <div className="flex gap-2.5">
      <Avatar name={sender} hidden={!showAvatar} />
      <div className="flex-1 max-w-[70%]">
        {showAvatar ? (
          <div className="text-[11px] text-feishu-muted mb-1 ml-1">
            @{sender} 正在工作
          </div>
        ) : null}
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tl-md bg-white border border-feishu-border text-feishu-text text-sm">
          <span className="whitespace-pre-wrap break-words">
            <MentionText text={bubble.text} />
          </span>
          <span className="caret" />
        </div>
      </div>
    </div>
  );
}

export function ChatPanel({
  view,
  onAnswer,
}: {
  view: ConversationView;
  onAnswer?: (
    question: QuestionInfo,
    answer: string | string[] | boolean,
    text: string,
  ) => void;
}) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [view.messages.length, view.notifications.length, view.workingBubbles.length]);

  const items: TimelineItem[] = [
    ...view.messages.map((m) => ({ type: "message" as const, seq: m.seq, data: m })),
    ...view.notifications.map((n) => ({ type: "notification" as const, seq: n.seq, data: n })),
    ...view.workingBubbles.map((b, i) => ({ type: "working" as const, seq: 1000000 + i, data: b })),
    ...Object.values(view.questions).map((q) => ({
      type: "question" as const,
      seq: q.seq,
      data: q,
    })),
  ].sort((a, b) => a.seq - b.seq);
  const timeline = mergeConsecutiveJoins(items);

  if (items.length === 0) {
    return (
      <div className="flex-1 min-h-0 flex items-center justify-center bg-feishu-bg/50">
        <div className="text-sm text-feishu-muted">输入消息开始对话</div>
      </div>
    );
  }

  const rendered: React.ReactNode[] = [];
  let thinkingBlocks: string[] = [];
  let thinkingId = "";
  let lastAuthor: string | null = null;

  const flushThinking = (): void => {
    if (thinkingBlocks.length === 0) return;
    const showAvatar = lastAuthor !== "规划大脑";
    rendered.push(
      <div key={`thinking-${thinkingId}`} className="animate-[fade-in_0.3s_ease-out]">
        <ThinkingItem blocks={thinkingBlocks} showAvatar={showAvatar} />
      </div>,
    );
    if (showAvatar) lastAuthor = "规划大脑";
    thinkingBlocks = [];
    thinkingId = "";
  };

  const accumulateThinking = (msg: ChatMessage): void => {
    thinkingBlocks = [...thinkingBlocks, msg.text];
    if (!thinkingId) thinkingId = msg.id;
  };

  for (const item of timeline) {
    if (item.type === "message" && item.data.thinking) {
      accumulateThinking(item.data);
      continue;
    }

    if (item.type === "message") {
      if (item.data.role === "assistant") {
        const showAvatar = lastAuthor !== "规划大脑";
        rendered.push(
          <div key={`msg-${item.data.id}`} className="animate-[fade-in-up_0.2s_ease-out]">
            <AssistantTextBubble
              msg={item.data}
              thinking={thinkingBlocks.length ? thinkingBlocks : null}
              showAvatar={showAvatar}
            />
          </div>,
        );
        lastAuthor = "规划大脑";
        thinkingBlocks = [];
        thinkingId = "";
        continue;
      }
      flushThinking();
      if (item.data.role === "user") {
        lastAuthor = null;
        rendered.push(
          <div key={`msg-${item.data.id}`} className="animate-[fade-in-up_0.2s_ease-out]">
            <UserBubble msg={item.data} />
          </div>,
        );
      } else {
        const sender = item.data.sender || "agent";
        const showAvatar = lastAuthor !== sender;
        rendered.push(
          <div key={`msg-${item.data.id}`} className="animate-[fade-in-up_0.2s_ease-out]">
            <AgentBubble msg={item.data} showAvatar={showAvatar} />
          </div>,
        );
        lastAuthor = sender;
      }
    } else if (item.type === "notification") {
      flushThinking();
      lastAuthor = null;
      rendered.push(
        <div key={`notif-${item.data.id}`} className="animate-[fade-in_0.3s_ease-out]">
          <NotificationItem notif={item.data} />
        </div>,
      );
    } else if (item.type === "question") {
      flushThinking();
      const requester = item.data.requester || "规划大脑";
      const showAvatar = lastAuthor !== requester;
      rendered.push(
        <div key={`question-${item.data.id}`} className="animate-[fade-in-up_0.2s_ease-out]">
          <QuestionCard
            question={item.data}
            showAvatar={showAvatar}
            onAnswer={(question, answer, text) => onAnswer?.(question, answer, text)}
          />
        </div>,
      );
      lastAuthor = requester;
    } else {
      flushThinking();
      const author = item.data.author || "agent";
      const showAvatar = lastAuthor !== author;
      rendered.push(
        <div key={`working-${item.data.artifactId}`} className="animate-[fade-in_0.3s_ease-out]">
          <WorkingItem bubble={item.data} showAvatar={showAvatar} />
        </div>,
      );
      lastAuthor = author;
    }
  }
  flushThinking();

  return (
    <div className="flex-1 min-h-0 overflow-y-auto bg-feishu-bg/50 px-4 py-4">
      <div className="flex flex-col gap-3 max-w-3xl mx-auto">
        {rendered}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
