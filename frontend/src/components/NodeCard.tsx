import type { NodeView } from "../lib/taskView";

export function NodeCard({
  node,
  onRetry,
}: {
  node: NodeView;
  onRetry: (nodeId: string) => void;
}) {
  return (
    <div className={`node-card status-${node.status} ${node.superseded ? "superseded" : ""}`}>
      <div className="node-head">
        <span className={`dot ${node.status}`} />
        <span className="node-name">{node.name}</span>
        {node.agentName ? <span className="badge">{node.agentName}</span> : null}
        <span className="node-status">{node.status}</span>
        {node.attempt > 1 ? <span className="muted">第 {node.attempt} 次尝试</span> : null}
        {node.status === "failed" ? (
          <button type="button" className="button small" onClick={() => onRetry(node.id)}>
            重试
          </button>
        ) : null}
      </div>
      {node.inputText ? <div className="node-input">→ {node.inputText}</div> : null}
      {node.outputText ? <div className="node-output">{node.outputText}</div> : null}
      {node.error ? <div className="node-error">{node.error}</div> : null}
    </div>
  );
}
