import { useMemo, useRef, useState } from "react";
import { AtSign, CornerDownLeft, X } from "lucide-react";

import type { RoomMemberDto, RoomMessageDto } from "../../lib/types";
import { Avatar } from "../Avatar";

export interface RoomSendInput {
  text: string;
  mentions: string[];
  quote_id?: string;
  interrupt?: boolean;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function extractMentions(text: string, members: RoomMemberDto[]): string[] {
  return members
    .map((member) => member.agent_name)
    .filter((name) =>
      new RegExp(`@${escapeRegExp(name)}(?![A-Za-z0-9_-])`).test(text),
    );
}

export function RoomComposer({
  members,
  replyTo,
  interrupt,
  onToggleInterrupt,
  onCancelReply,
  onSend,
}: {
  members: RoomMemberDto[];
  replyTo: RoomMessageDto | null;
  interrupt: boolean;
  onToggleInterrupt: (value: boolean) => void;
  onCancelReply: () => void;
  onSend: (input: RoomSendInput) => Promise<void>;
}) {
  const [text, setText] = useState("");
  const [query, setQuery] = useState<string | null>(null);
  const [active, setActive] = useState(0);
  const [busy, setBusy] = useState(false);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const suggestions = useMemo(() => {
    if (query === null) return [];
    return members
      .filter((member) =>
        member.agent_name.toLowerCase().startsWith(query.toLowerCase()),
      )
      .slice(0, 6);
  }, [members, query]);

  function updateQuery(value: string, caret: number) {
    const before = value.slice(0, caret);
    const match = /@([A-Za-z0-9_-]*)$/.exec(before);
    setQuery(match ? match[1] : null);
    setActive(0);
  }

  function insertMention(name: string) {
    const caret = textareaRef.current?.selectionStart ?? text.length;
    const before = text
      .slice(0, caret)
      .replace(/@([A-Za-z0-9_-]*)$/, `@${name} `);
    setText(before + text.slice(caret));
    setQuery(null);
    textareaRef.current?.focus();
  }

  async function submit() {
    const value = text.trim();
    if (!value || busy) return;
    setBusy(true);
    try {
      await onSend({
        text: value,
        mentions: extractMentions(value, members),
        ...(replyTo ? { quote_id: replyTo.id } : {}),
        ...(replyTo && interrupt ? { interrupt: true } : {}),
      });
      setText("");
      setQuery(null);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex flex-col gap-2 px-6 py-3 bg-white border-t border-feishu-border flex-shrink-0">
      {replyTo ? (
        <div className="flex items-center gap-2 w-full px-3 py-2 rounded-lg bg-feishu-primary-soft border border-feishu-primary-border text-xs text-feishu-text-secondary">
          <span className="flex-1 truncate">
            引用 {replyTo.sender ?? replyTo.role}：{replyTo.text.slice(0, 40)}
          </span>
          {replyTo.node_id ? (
            <label className="flex items-center gap-1.5 shrink-0 cursor-pointer">
              <input
                type="checkbox"
                checked={interrupt}
                onChange={(event) => onToggleInterrupt(event.target.checked)}
                className="accent-feishu-primary"
              />
              打断当前工作
            </label>
          ) : null}
          <button
            type="button"
            onClick={onCancelReply}
            className="flex items-center justify-center w-5 h-5 rounded text-feishu-muted hover:bg-feishu-border-light hover:text-feishu-text transition-colors shrink-0"
          >
            <X size={12} />
          </button>
        </div>
      ) : null}

      <div className="flex gap-2.5 items-end">
        <div className="flex items-center gap-1 pb-1.5">
          <button
            type="button"
            title="@ 提及"
            onClick={() => {
              const ta = textareaRef.current;
              if (!ta) return;
              const caret = ta.selectionStart;
              const before = text.slice(0, caret);
              const after = text.slice(caret);
              const newText = `${before}@${after}`;
              setText(newText);
              setQuery("");
              requestAnimationFrame(() => {
                ta.selectionStart = ta.selectionEnd = caret + 1;
                ta.focus();
              });
            }}
            className="flex items-center justify-center w-8 h-8 rounded-lg text-feishu-muted hover:bg-feishu-bg hover:text-feishu-primary transition-colors"
          >
            <AtSign size={18} />
          </button>
        </div>

        <div className="flex-1 relative">
          <textarea
            ref={textareaRef}
            className="w-full resize-none border border-feishu-border rounded-xl px-3.5 py-2.5 text-sm font-sans outline-none focus:border-feishu-primary focus:ring-2 focus:ring-feishu-primary/10 transition-all bg-feishu-bg/50 placeholder:text-feishu-muted"
            rows={2}
            value={text}
            placeholder="输入消息，@ 指派 Agent，Enter 发送"
            onChange={(event) => {
              setText(event.target.value);
              updateQuery(event.target.value, event.target.selectionStart ?? 0);
            }}
            onKeyDown={(event) => {
              if (suggestions.length > 0) {
                if (event.key === "ArrowDown") {
                  event.preventDefault();
                  setActive((index) => (index + 1) % suggestions.length);
                  return;
                }
                if (event.key === "ArrowUp") {
                  event.preventDefault();
                  setActive(
                    (index) =>
                      (index - 1 + suggestions.length) % suggestions.length,
                  );
                  return;
                }
                if (event.key === "Enter" || event.key === "Tab") {
                  event.preventDefault();
                  insertMention(suggestions[active].agent_name);
                  return;
                }
                if (event.key === "Escape") {
                  setQuery(null);
                  return;
                }
              }
              if (event.key === "Enter" && !event.shiftKey) {
                event.preventDefault();
                void submit();
              }
            }}
          />

          {suggestions.length > 0 ? (
            <div className="absolute bottom-full left-0 mb-1 bg-white border border-feishu-border rounded-lg shadow-lg overflow-hidden min-w-[180px] z-10">
              {suggestions.map((member, index) => (
                <button
                  key={member.agent_name}
                  type="button"
                  aria-label={`@${member.agent_name}`}
                  className={`flex items-center gap-2 w-full px-2.5 py-2 text-left transition-colors ${
                    index === active
                      ? "bg-feishu-primary-soft text-feishu-primary"
                      : "hover:bg-feishu-bg text-feishu-text"
                  }`}
                  onMouseDown={(event) => {
                    event.preventDefault();
                    insertMention(member.agent_name);
                  }}
                >
                  <Avatar name={member.agent_name} size={24} />
                  <span className="text-[13px]">@{member.agent_name}</span>
                </button>
              ))}
            </div>
          ) : null}
        </div>

        <button
          type="button"
          disabled={busy || !text.trim()}
          onClick={() => void submit()}
          className="flex items-center gap-1.5 px-4 py-2.5 rounded-xl bg-feishu-primary text-white text-[13px] font-medium hover:bg-feishu-primary-dark disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          发送
          <CornerDownLeft size={14} />
        </button>
      </div>
    </div>
  );
}
