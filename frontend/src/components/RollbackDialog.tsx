import { useEffect, useState } from "react";
import { X, Eye, AlertTriangle } from "lucide-react";

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
      .catch((exc: unknown) =>
        setError(exc instanceof Error ? exc.message : "加载失败"),
      );
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
    <div
      className="fixed inset-0 bg-black/45 flex items-center justify-center z-10"
      onClick={onClose}
    >
      <div
        className="bg-white rounded-2xl shadow-xl w-full max-w-[480px] mx-4 p-5 flex flex-col gap-3"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between">
          <h3 className="font-bold text-base text-feishu-text m-0">
            回退到 Checkpoint
          </h3>
          <button
            type="button"
            onClick={onClose}
            className="flex items-center justify-center w-7 h-7 rounded-lg text-feishu-muted hover:bg-feishu-bg transition-colors"
          >
            <X size={16} />
          </button>
        </div>

        {checkpoints.length === 0 ? (
          <p className="text-feishu-muted text-sm">暂无可用 checkpoint</p>
        ) : null}

        <select
          className="border border-feishu-border rounded-lg px-2.5 py-2 text-[13px] font-sans outline-none focus:border-feishu-primary bg-white"
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
          <div className="text-[13px] bg-feishu-bg rounded-lg px-3 py-2.5">
            将重置 {resetNodeIds.length} 个节点：
            <ul className="m-1.5 0 0 pl-4 max-h-40 overflow-y-auto list-disc">
              {resetNodeIds.map((nodeId) => (
                <li key={nodeId}>{nodeId}</li>
              ))}
            </ul>
          </div>
        ) : null}

        {error ? (
          <div className="flex items-center gap-1.5 text-feishu-danger text-[13px]">
            <AlertTriangle size={14} />
            {error}
          </div>
        ) : null}

        <div className="flex justify-end gap-2">
          <button
            type="button"
            onClick={onClose}
            className="px-3.5 py-2 rounded-lg border border-feishu-border bg-white text-feishu-text text-[13px] hover:bg-feishu-bg transition-colors"
          >
            取消
          </button>
          <button
            type="button"
            disabled={busy || !selected}
            onClick={() => void dryRun()}
            className="flex items-center gap-1 px-3.5 py-2 rounded-lg border border-feishu-border bg-white text-feishu-text text-[13px] hover:bg-feishu-bg disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            <Eye size={14} />
            预览影响
          </button>
          <button
            type="button"
            disabled={busy || !selected}
            onClick={() => void restart()}
            className="px-3.5 py-2 rounded-lg bg-feishu-danger text-white text-[13px] hover:bg-feishu-danger/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            确认回退
          </button>
        </div>
      </div>
    </div>
  );
}
