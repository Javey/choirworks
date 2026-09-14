import { RotateCcw, Zap } from "lucide-react";

import type { NodeView } from "../lib/taskView";

const STATUS_DOT: Record<string, string> = {
  pending: "bg-feishu-muted",
  ready: "bg-feishu-primary",
  dispatched: "bg-feishu-primary",
  working: "bg-feishu-primary",
  input_required: "bg-feishu-warn",
  completed: "bg-feishu-success",
  failed: "bg-feishu-danger",
  canceled: "bg-feishu-muted",
  invalidated: "bg-feishu-muted",
};

const BORDER_COLOR: Record<string, string> = {
  pending: "border-l-feishu-primary",
  ready: "border-l-feishu-primary",
  dispatched: "border-l-feishu-primary",
  working: "border-l-feishu-primary",
  input_required: "border-l-feishu-warn",
  completed: "border-l-feishu-success",
  failed: "border-l-feishu-danger",
  canceled: "border-l-feishu-muted",
  invalidated: "border-l-feishu-muted",
};

export function NodeCard({
  node,
  onRetry,
}: {
  node: NodeView;
  onRetry: (nodeId: string) => void;
}) {
  const isSuperseded = node.superseded;
  const dotClass = STATUS_DOT[node.status] ?? "bg-feishu-muted";
  const borderClass = BORDER_COLOR[node.status] ?? "border-l-feishu-muted";

  return (
    <div
      className={`self-start w-full max-w-[640px] bg-white border border-feishu-border border-l-[3px] ${borderClass} rounded-xl px-3.5 py-2.5 text-[13px] flex flex-col gap-1.5 shadow-sm ${isSuperseded ? "opacity-60" : ""}`}
    >
      <div className="flex items-center gap-2 flex-wrap">
        <span className={`w-2 h-2 rounded-full ${dotClass}`} />
        <span className="font-semibold text-feishu-text">{node.name}</span>
        {node.agentName ? (
          <span className="inline-flex items-center px-1.5 py-0.5 rounded-full bg-feishu-bg border border-feishu-border text-[11px] text-feishu-text-secondary">
            {node.agentName}
          </span>
        ) : null}
        {node.derived ? (
          <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded-full bg-feishu-primary-soft border border-feishu-primary-border text-[11px] text-feishu-primary">
            <Zap size={10} />
            协助
          </span>
        ) : null}
        <span className="text-feishu-muted text-[12px]">{node.status}</span>
        {node.attempt > 1 ? (
          <span className="text-feishu-muted text-[12px]">第 {node.attempt} 次尝试</span>
        ) : null}
        {node.status === "failed" ? (
          <button
            type="button"
            onClick={() => onRetry(node.id)}
            className="flex items-center gap-1 px-2 py-1 rounded-lg text-xs text-feishu-primary hover:bg-feishu-primary-soft transition-colors ml-auto"
          >
            <RotateCcw size={12} />
            重试
          </button>
        ) : null}
      </div>
      {node.inputText ? (
        <div className="text-feishu-muted">→ {node.inputText}</div>
      ) : null}
      {node.outputText ? (
        <div
          className={`whitespace-pre-wrap break-words bg-feishu-bg rounded-lg px-2.5 py-2 text-[13px] ${
            node.status === "working" || node.status === "dispatched"
              ? "after:content-['▍'] after:text-feishu-primary after:animate-[caret-blink_1s_step-end_infinite]"
              : ""
          }`}
        >
          {node.outputText}
        </div>
      ) : null}
      {node.error ? (
        <div className="text-feishu-danger whitespace-pre-wrap break-words">
          {node.error}
        </div>
      ) : null}
    </div>
  );
}
