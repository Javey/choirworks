import { useState } from "react";
import { AlertTriangle, CheckCircle2, XCircle } from "lucide-react";

import type { InterventionView } from "../lib/taskView";

export function InterventionCard({
  taskId,
  intervention,
  onAnswer,
}: {
  taskId: string;
  intervention: InterventionView;
  onAnswer: (
    taskId: string,
    interventionId: string,
    text: string,
  ) => void | Promise<void>;
}) {
  const [text, setText] = useState("");
  const [busy, setBusy] = useState(false);
  const pending = intervention.status === "pending";

  const submit = async () => {
    const value = text.trim();
    if (!value || busy) return;
    setBusy(true);
    try {
      await onAnswer(taskId, intervention.id, value);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div
      className={`self-start w-full max-w-[640px] rounded-xl px-3.5 py-2.5 flex flex-col gap-2 text-[13px] shadow-sm border ${
        pending
          ? "bg-feishu-warn-soft border-feishu-warn/30"
          : "bg-white border-feishu-border"
      }`}
    >
      <div className="flex items-center gap-2">
        <AlertTriangle size={14} className="text-feishu-warn shrink-0" />
        <span className="inline-flex items-center px-2 py-0.5 rounded-full bg-feishu-warn-soft text-feishu-warn border border-feishu-warn/20 text-[11px] font-medium">
          需要人工介入
        </span>
        <span className="text-feishu-muted text-[12px]">
          {intervention.source} · {intervention.policy}
        </span>
      </div>
      <div className="font-semibold text-feishu-text">
        {intervention.questionText}
      </div>
      {pending && intervention.assignedTo ? (
        <div className="text-feishu-text-secondary">
          已指派 {intervention.assignedTo} 处理中…
        </div>
      ) : pending ? (
        <div className="flex gap-2 items-end">
          <textarea
            rows={2}
            value={text}
            placeholder="输入回答"
            onChange={(event) => setText(event.target.value)}
            className="flex-1 resize-none border border-feishu-border rounded-lg px-2.5 py-2 text-[13px] font-sans outline-none focus:border-feishu-primary bg-white"
          />
          <button
            type="button"
            disabled={busy || !text.trim()}
            onClick={() => void submit()}
            className="px-3.5 py-2 rounded-lg bg-feishu-primary text-white text-[13px] font-medium hover:bg-feishu-primary-dark disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            提交回答
          </button>
        </div>
      ) : intervention.status === "failed" ? (
        <div className="flex items-center gap-1.5 text-feishu-danger">
          <XCircle size={14} />
          协助失败，任务将重试或重规划
        </div>
      ) : (
        <div className="flex items-start gap-1.5 text-feishu-text-secondary">
          <CheckCircle2 size={14} className="text-feishu-success mt-0.5 shrink-0" />
          <span>
            已由 {intervention.responder ?? "unknown"} 回答：
            <span className="text-feishu-text">{intervention.answerText ?? "（空）"}</span>
          </span>
        </div>
      )}
    </div>
  );
}
