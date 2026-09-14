import { Ban, History, PlayCircle } from "lucide-react";

import type { TaskView } from "../lib/taskView";
import { buildTimeline } from "../lib/timeline";
import { InterventionCard } from "./InterventionCard";
import { NodeCard } from "./NodeCard";
import { statusLabel } from "./Sidebar";

const STATUS_BADGE: Record<string, string> = {
  pending: "bg-feishu-bg text-feishu-muted border-feishu-border",
  planning: "bg-feishu-primary-soft text-feishu-primary border-feishu-primary-border",
  running: "bg-feishu-primary-soft text-feishu-primary border-feishu-primary-border",
  awaiting_input: "bg-feishu-warn-soft text-feishu-warn border-feishu-warn/20",
  completed: "bg-feishu-success-soft text-feishu-success border-feishu-success/20",
  failed: "bg-feishu-danger-soft text-feishu-danger border-feishu-danger/20",
  canceled: "bg-feishu-bg text-feishu-muted border-feishu-border",
};

export function Thread({
  views,
  onAnswer,
  onRetry,
  onCancel,
  onRollback,
}: {
  views: TaskView[];
  onAnswer: (taskId: string, interventionId: string, text: string) => Promise<void>;
  onRetry: (taskId: string, nodeId: string) => void;
  onCancel: (taskId: string) => void;
  onRollback: (taskId: string) => void;
}) {
  const items = buildTimeline(views);
  const latest = views.at(-1);

  return (
    <div className="flex-1 flex flex-col min-h-0">
      <header className="flex items-center justify-between px-4 h-12 bg-white border-b border-feishu-border flex-shrink-0">
        <div className="flex items-center gap-2 min-w-0">
          <span className="font-semibold text-sm text-feishu-text truncate">
            {latest ? latest.request.slice(0, 40) : "新对话"}
          </span>
          {latest ? (
            <span
              className={`inline-flex items-center px-2 py-0.5 rounded-full text-[11px] border ${STATUS_BADGE[latest.status] ?? STATUS_BADGE.pending}`}
            >
              {statusLabel(latest.status)}
            </span>
          ) : null}
        </div>
        <div className="flex items-center gap-1.5">
          {latest && !["completed", "failed", "canceled"].includes(latest.status) ? (
            <button
              type="button"
              onClick={() => onCancel(latest.id)}
              className="flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs text-feishu-text-secondary hover:bg-feishu-bg hover:text-feishu-danger transition-colors"
            >
              <Ban size={12} />
              取消
            </button>
          ) : null}
          {latest ? (
            <button
              type="button"
              onClick={() => onRollback(latest.id)}
              className="flex items-center gap-1 px-2.5 py-1 rounded-lg text-xs text-feishu-text-secondary hover:bg-feishu-bg hover:text-feishu-primary transition-colors"
            >
              <History size={12} />
              回退
            </button>
          ) : null}
        </div>
      </header>

      <div className="flex-1 overflow-y-auto p-4">
        <div className="flex flex-col gap-3.5 max-w-[840px] w-full mx-auto">
          {items.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-20 text-center">
              <div className="flex items-center justify-center w-16 h-16 rounded-full bg-feishu-primary-soft mb-4">
                <PlayCircle size={28} className="text-feishu-primary" />
              </div>
              <h2 className="text-lg font-bold text-feishu-text mb-2">
                开始一个任务
              </h2>
              <p className="text-sm text-feishu-muted max-w-xs leading-relaxed">
                描述你的需求，平台会拆解为多 Agent 的执行计划并实时展示过程。
              </p>
            </div>
          ) : null}

          {items.map((item) => {
            switch (item.kind) {
              case "user":
                return (
                  <div
                    key={item.key}
                    className="self-end max-w-[78%] px-3.5 py-2 rounded-[12px] rounded-br-[4px] bg-feishu-primary text-white text-sm leading-relaxed whitespace-pre-wrap break-words"
                  >
                    {item.text}
                  </div>
                );
              case "plan":
                return (
                  <details
                    key={item.key}
                    className="bg-white border border-feishu-border rounded-xl px-3.5 py-2.5 text-[13px] shadow-sm"
                  >
                    <summary className="cursor-pointer text-feishu-text-secondary">
                      执行计划：{item.nodes.length} 个节点
                      {item.rationale ? (
                        <span className="text-feishu-muted"> · {item.rationale}</span>
                      ) : null}
                    </summary>
                    <ol className="m-2.5 0 1 pl-5 flex flex-col gap-1.5 list-decimal">
                      {item.nodes.map((node, index) => (
                        <li key={`${item.key}:${index}`}>
                          <span className="font-medium">{node.name}</span>
                          {node.agentName ? (
                            <span className="ml-1.5 inline-flex items-center px-1.5 py-0.5 rounded-full bg-feishu-bg border border-feishu-border text-[11px] text-feishu-text-secondary">
                              {node.agentName}
                            </span>
                          ) : null}
                        </li>
                      ))}
                    </ol>
                  </details>
                );
              case "node":
                return (
                  <NodeCard
                    key={item.key}
                    node={item.node}
                    onRetry={(nodeId) => onRetry(item.taskId, nodeId)}
                  />
                );
              case "intervention":
                return (
                  <InterventionCard
                    key={item.key}
                    taskId={item.taskId}
                    intervention={item.intervention}
                    onAnswer={onAnswer}
                  />
                );
              case "note":
                return (
                  <div
                    key={item.key}
                    className={`self-start text-xs px-2.5 py-1 rounded-lg ${
                      item.level === "warn"
                        ? "text-feishu-warn bg-feishu-warn-soft"
                        : "text-feishu-muted bg-feishu-bg"
                    }`}
                  >
                    {item.text}
                  </div>
                );
              case "result":
                return (
                  <div
                    key={item.key}
                    className={`self-start max-w-[78%] px-3.5 py-2 rounded-[12px] bg-white border border-feishu-border text-sm leading-relaxed whitespace-pre-wrap break-words ${
                      item.status === "completed"
                        ? "border-l-[3px] border-l-feishu-success"
                        : item.status === "failed"
                          ? "border-l-[3px] border-l-feishu-danger"
                          : "border-l-[3px] border-l-feishu-muted"
                    }`}
                  >
                    {item.status === "completed"
                      ? (item.text ?? "任务完成")
                      : item.status === "failed"
                        ? `任务失败：${item.error ?? "未知错误"}`
                        : "任务已取消"}
                  </div>
                );
            }
          })}
        </div>
      </div>
    </div>
  );
}
