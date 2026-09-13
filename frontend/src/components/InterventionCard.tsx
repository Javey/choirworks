import { useState } from "react";

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
    <div className={`intervention ${pending ? "pending" : "resolved"}`}>
      <div className="intervention-head">
        <span className="badge warn">需要人工介入</span>
        <span className="muted">
          {intervention.source} · {intervention.policy}
        </span>
      </div>
      <div className="intervention-question">{intervention.questionText}</div>
      {pending && intervention.assignedTo ? (
        <div className="intervention-waiting">
          已指派 {intervention.assignedTo} 处理中…
        </div>
      ) : pending ? (
        <div className="intervention-answer">
          <textarea
            rows={2}
            value={text}
            placeholder="输入回答"
            onChange={(event) => setText(event.target.value)}
          />
          <button
            type="button"
            className="button primary"
            disabled={busy || !text.trim()}
            onClick={() => void submit()}
          >
            提交回答
          </button>
        </div>
      ) : intervention.status === "failed" ? (
        <div className="intervention-resolved">协助失败，任务将重试或重规划</div>
      ) : (
        <div className="intervention-resolved">
          已由 {intervention.responder ?? "unknown"} 回答：
          <span>{intervention.answerText ?? "（空）"}</span>
        </div>
      )}
    </div>
  );
}
