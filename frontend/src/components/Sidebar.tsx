import { Plus, Search, MessageSquare } from "lucide-react";

import type { ConversationSummaryDto } from "../lib/types";

const STATUS_LABELS: Record<string, string> = {
  pending: "待规划",
  planning: "规划中",
  running: "执行中",
  awaiting_input: "等待输入",
  completed: "已完成",
  failed: "失败",
  canceled: "已取消",
};

export function statusLabel(status: string): string {
  return STATUS_LABELS[status] ?? status;
}

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const minutes = Math.floor(diff / 60000);
  if (minutes < 1) return "刚刚";
  if (minutes < 60) return `${minutes}分钟前`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}小时前`;
  const days = Math.floor(hours / 24);
  if (days < 7) return `${days}天前`;
  const date = new Date(iso);
  return `${date.getMonth() + 1}/${date.getDate()}`;
}

const STATUS_COLORS: Record<string, string> = {
  pending: "bg-feishu-muted",
  planning: "bg-feishu-primary",
  running: "bg-feishu-primary",
  awaiting_input: "bg-feishu-warn",
  completed: "bg-feishu-success",
  failed: "bg-feishu-danger",
  canceled: "bg-feishu-muted",
};

const AVATAR_BG: Record<string, string> = {
  pending: "#8f959e",
  planning: "#3370ff",
  running: "#3370ff",
  awaiting_input: "#ff9500",
  completed: "#34c759",
  failed: "#f54a45",
  canceled: "#8f959e",
};

export function Sidebar({
  conversations,
  activeId,
  onSelect,
  onNew,
}: {
  conversations: ConversationSummaryDto[];
  activeId: string | null;
  onSelect: (id: string) => void;
  onNew: () => void;
}) {
  return (
    <aside className="flex w-[280px] shrink-0 flex-col bg-feishu-sidebar border-r border-feishu-border">
      <div className="flex items-center justify-between px-4 h-14 border-b border-feishu-border">
        <span className="font-bold text-[15px] text-feishu-text">
          ChoirWorks
        </span>
        <button
          type="button"
          onClick={onNew}
          className="flex items-center gap-1 px-2.5 py-1.5 rounded-lg bg-feishu-primary text-white text-xs font-medium hover:bg-feishu-primary-dark transition-colors"
        >
          <Plus size={14} />
          新对话
        </button>
      </div>

      <div className="px-3 py-2">
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-white border border-feishu-border">
          <Search size={14} className="text-feishu-muted" />
          <input
            type="text"
            placeholder="搜索对话"
            className="flex-1 bg-transparent text-xs outline-none placeholder:text-feishu-muted text-feishu-text"
          />
        </div>
      </div>

      <nav className="flex-1 overflow-y-auto px-2 pb-2 flex flex-col gap-0.5">
        {conversations.map((conversation) => {
          const isActive = conversation.id === activeId;
          const label = conversation.title || "新对话";
          const bgColor = AVATAR_BG[conversation.last_status] ?? "#8f959e";
          const statusColor = STATUS_COLORS[conversation.last_status] ?? "bg-feishu-muted";
          return (
            <button
              key={conversation.id}
              type="button"
              onClick={() => onSelect(conversation.id)}
              className={`group relative flex items-center gap-3 w-full px-2 py-2.5 rounded-lg text-left transition-colors ${
                isActive
                  ? "bg-feishu-sidebar-active"
                  : "hover:bg-white/60"
              }`}
            >
              {isActive && (
                <span className="absolute left-0 top-1/2 -translate-y-1/2 h-6 w-[3px] rounded-r bg-feishu-primary" />
              )}
              <div className="relative shrink-0">
                <div
                  className="flex items-center justify-center w-10 h-10 rounded-full text-white font-semibold text-sm"
                  style={{ background: bgColor }}
                >
                  {label.slice(0, 1).toUpperCase()}
                </div>
                <span
                  className={`absolute bottom-0 right-0 w-3 h-3 rounded-full border-2 border-feishu-sidebar ${statusColor}`}
                />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <span className="text-[13px] font-medium text-feishu-text truncate">
                    {label}
                  </span>
                  <span className="text-[11px] text-feishu-muted shrink-0">
                    {timeAgo(conversation.updated_at)}
                  </span>
                </div>
                <div className="text-[12px] text-feishu-muted truncate mt-0.5">
                  {statusLabel(conversation.last_status)}
                </div>
              </div>
            </button>
          );
        })}
        {conversations.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 px-4 text-center">
            <MessageSquare size={32} className="text-feishu-muted mb-2" />
            <p className="text-[13px] text-feishu-muted">
              还没有对话
            </p>
            <p className="text-[12px] text-feishu-muted mt-1">
              输入一条消息开始协作
            </p>
          </div>
        ) : null}
      </nav>
    </aside>
  );
}
