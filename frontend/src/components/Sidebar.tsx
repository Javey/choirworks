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
  if (minutes < 60) return `${minutes} 分钟前`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} 小时前`;
  return `${Math.floor(hours / 24)} 天前`;
}

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
    <aside className="sidebar">
      <div className="sidebar-head">
        <span className="brand">Agent Hub</span>
        <button type="button" className="button primary small" onClick={onNew}>
          ＋ 新对话
        </button>
      </div>
      <nav className="conversation-list">
        {conversations.map((conversation) => (
          <button
            key={conversation.id}
            type="button"
            className={`conversation-item ${conversation.id === activeId ? "active" : ""}`}
            onClick={() => onSelect(conversation.id)}
          >
            <span className={`dot ${conversation.last_status}`} />
            <span className="conversation-title">{conversation.title}</span>
            <span className="conversation-meta">{timeAgo(conversation.updated_at)}</span>
          </button>
        ))}
        {conversations.length === 0 ? (
          <div className="sidebar-empty">还没有对话，输入一条消息开始</div>
        ) : null}
      </nav>
    </aside>
  );
}
