export function formatTime(iso: string): string {
  const date = new Date(iso);
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${hours}:${minutes}`;
}

export function formatTimeDivider(iso: string): string {
  const date = new Date(iso);
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const yesterday = new Date(today.getTime() - 86400000);
  const target = new Date(date.getFullYear(), date.getMonth(), date.getDate());

  const time = formatTime(iso);

  if (target.getTime() === today.getTime()) {
    return time;
  }
  if (target.getTime() === yesterday.getTime()) {
    return `昨天 ${time}`;
  }
  return `${date.getMonth() + 1}月${date.getDate()}日 ${time}`;
}

export function TimeDivider({ text }: { text: string }) {
  return (
    <div className="flex items-center justify-center py-2">
      <span className="px-3 py-1 rounded-full bg-feishu-border-light text-xs text-feishu-muted select-none">
        {text}
      </span>
    </div>
  );
}
