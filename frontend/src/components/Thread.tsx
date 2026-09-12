import type { TaskView } from "../lib/taskView";
import { buildTimeline } from "../lib/timeline";
import { InterventionCard } from "./InterventionCard";
import { NodeCard } from "./NodeCard";
import { statusLabel } from "./Sidebar";

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
    <div className="main">
      <header className="thread-head">
        <div className="thread-title">
          <span>{latest ? latest.request.slice(0, 40) : "新对话"}</span>
          {latest ? (
            <span className={`badge status-${latest.status}`}>
              {statusLabel(latest.status)}
            </span>
          ) : null}
          {latest && latest.connection === "reconnecting" ? (
            <span className="badge warn">重连中…</span>
          ) : null}
        </div>
        <div className="thread-actions">
          {latest && !["completed", "failed", "canceled"].includes(latest.status) ? (
            <button
              type="button"
              className="button small"
              onClick={() => onCancel(latest.id)}
            >
              取消任务
            </button>
          ) : null}
          {latest ? (
            <button
              type="button"
              className="button small"
              onClick={() => onRollback(latest.id)}
            >
              回退
            </button>
          ) : null}
        </div>
      </header>

      <div className="thread-body">
        {items.length === 0 ? (
          <div className="thread-empty">
            <h2>开始一个任务</h2>
            <p>描述你的需求，平台会拆解为多 Agent 的执行计划并实时展示过程。</p>
          </div>
        ) : null}

        {items.map((item) => {
          switch (item.kind) {
            case "user":
              return (
                <div key={item.key} className="bubble user">
                  {item.text}
                </div>
              );
            case "plan":
              return (
                <details key={item.key} className="plan-card">
                  <summary>
                    执行计划：{item.nodes.length} 个节点
                    {item.rationale ? <span className="muted"> · {item.rationale}</span> : null}
                  </summary>
                  <ol>
                    {item.nodes.map((node, index) => (
                      <li key={`${item.key}:${index}`}>
                        <span className="node-name">{node.name}</span>
                        {node.agentName ? <span className="badge">{node.agentName}</span> : null}
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
                <div key={item.key} className={`note ${item.level}`}>
                  {item.text}
                </div>
              );
            case "result":
              return (
                <div key={item.key} className={`bubble result ${item.status}`}>
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
  );
}
