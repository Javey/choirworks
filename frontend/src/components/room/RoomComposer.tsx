import { useMemo, useRef, useState } from "react";

import type { RoomMemberDto, RoomMessageDto } from "../../lib/types";

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
    <div className="composer room-composer">
      {replyTo ? (
        <div className="reply-bar">
          <span className="reply-label">
            引用 {replyTo.sender ?? replyTo.role}：{replyTo.text.slice(0, 40)}
          </span>
          {replyTo.node_id ? (
            <label className="reply-interrupt">
              <input
                type="checkbox"
                checked={interrupt}
                onChange={(event) => onToggleInterrupt(event.target.checked)}
              />
              打断当前工作
            </label>
          ) : null}
          <button type="button" className="button small" onClick={onCancelReply}>
            取消
          </button>
        </div>
      ) : null}
      <div className="composer-row">
        <textarea
          ref={textareaRef}
          className="composer-input"
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
        <button
          type="button"
          className="button primary"
          disabled={busy || !text.trim()}
          onClick={() => void submit()}
        >
          发送
        </button>
      </div>
      {suggestions.length > 0 ? (
        <ul className="mention-list">
          {suggestions.map((member, index) => (
            <li key={member.agent_name}>
              <button
                type="button"
                className={index === active ? "active" : ""}
                onMouseDown={(event) => {
                  event.preventDefault();
                  insertMention(member.agent_name);
                }}
              >
                @{member.agent_name}
              </button>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
