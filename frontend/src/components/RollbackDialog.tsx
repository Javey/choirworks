import { useEffect, useState } from "react";

import { api } from "../api/client";
import type { CheckpointDto } from "../lib/types";

export function RollbackDialog({
  taskId,
  onClose,
  onDone,
}: {
  taskId: string;
  onClose: () => void;
  onDone: () => void;
}) {
  const [checkpoints, setCheckpoints] = useState<CheckpointDto[]>([]);
  const [selected, setSelected] = useState<string>("");
  const [resetNodeIds, setResetNodeIds] = useState<string[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    void api
      .listCheckpoints(taskId)
      .then((items) => {
        setCheckpoints([...items].reverse());
        setSelected(items.length > 0 ? items[items.length - 1].id : "");
      })
      .catch((exc: unknown) => setError(exc instanceof Error ? exc.message : "加载失败"));
  }, [taskId]);

  const dryRun = async () => {
    if (!selected) return;
    setBusy(true);
    setError(null);
    try {
      const report = await api.rollback(taskId, selected, "dry_run");
      setResetNodeIds(report.reset_node_ids);
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "预览失败");
    } finally {
      setBusy(false);
    }
  };

  const restart = async () => {
    if (!selected) return;
    setBusy(true);
    try {
      await api.rollback(taskId, selected, "restart");
      onDone();
      onClose();
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : "回退失败");
      setBusy(false);
    }
  };

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(event) => event.stopPropagation()}>
        <h3>回退到 Checkpoint</h3>
        {checkpoints.length === 0 ? <p className="muted">暂无可用 checkpoint</p> : null}
        <select
          className="select"
          value={selected}
          onChange={(event) => {
            setSelected(event.target.value);
            setResetNodeIds(null);
          }}
        >
          {checkpoints.map((checkpoint, index) => (
            <option key={checkpoint.id} value={checkpoint.id}>
              {index === 0 ? "最新" : `#${checkpoints.length - index}`} · v
              {checkpoint.plan_version} · {checkpoint.frontier.length} 个已完成节点
            </option>
          ))}
        </select>

        {resetNodeIds !== null ? (
          <div className="dry-run">
            将重置 {resetNodeIds.length} 个节点：
            <ul>
              {resetNodeIds.map((nodeId) => (
                <li key={nodeId}>{nodeId}</li>
              ))}
            </ul>
          </div>
        ) : null}
        {error ? <div className="error-text">{error}</div> : null}

        <div className="modal-actions">
          <button type="button" className="button" onClick={onClose}>
            取消
          </button>
          <button
            type="button"
            className="button"
            disabled={busy || !selected}
            onClick={() => void dryRun()}
          >
            预览影响
          </button>
          <button
            type="button"
            className="button danger"
            disabled={busy || !selected}
            onClick={() => void restart()}
          >
            确认回退
          </button>
        </div>
      </div>
    </div>
  );
}
