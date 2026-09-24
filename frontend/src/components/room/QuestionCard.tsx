import { useState } from "react";

import type { QuestionInfo } from "../../lib/conversationView";

const AVATAR_COLORS = [
  "#3370ff", "#7c3aed", "#34c759", "#ff9500",
  "#f54a45", "#00a6fb", "#e056fd", "#fa7268",
];

function avatarColor(name: string): string {
  const hash = name.split("").reduce((acc, ch) => acc + ch.charCodeAt(0), 0);
  return AVATAR_COLORS[hash % AVATAR_COLORS.length];
}

function RequesterAvatar({ name }: { name: string }) {
  const color = avatarColor(name || "orchestrator");
  return (
    <div
      className="flex items-center justify-center rounded-full text-white font-semibold shrink-0"
      style={{ background: color, width: 36, height: 36, fontSize: 14 }}
    >
      {(name || "O").slice(0, 1).toUpperCase()}
    </div>
  );
}

function answerLabel(question: QuestionInfo): string {
  const answer = question.answer;
  if (answer === null || answer === undefined) return "";
  if (Array.isArray(answer)) return answer.join("、");
  if (typeof answer === "boolean") {
    return question.kind === "confirm_cancel"
      ? answer
        ? "打断"
        : "保留"
      : answer
        ? "确认"
        : "取消";
  }
  return String(answer);
}

export function QuestionCard({
  question,
  onAnswer,
}: {
  question: QuestionInfo;
  onAnswer: (
    question: QuestionInfo,
    answer: string | string[] | boolean,
    text: string,
  ) => void;
}) {
  const [text, setText] = useState("");
  const [selected, setSelected] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const pending = question.status === "pending";
  const requester = question.requester || "orchestrator";

  function toggleOption(option: string) {
    setError(null);
    if (question.multi) {
      setSelected((prev) =>
        prev.includes(option) ? prev.filter((item) => item !== option) : [...prev, option],
      );
    } else {
      setSelected([option]);
    }
  }

  function submitInput() {
    const value = text.trim();
    if (!value) {
      setError("请输入内容");
      return;
    }
    onAnswer(question, value, value);
  }

  function submitSelect() {
    if (question.multi) {
      if (selected.length === 0) {
        setError("请至少选择一项");
        return;
      }
      onAnswer(question, selected, selected.join("、"));
      return;
    }
    if (selected.length === 0) {
      setError("请选择一项");
      return;
    }
    onAnswer(question, selected[0], selected[0]);
  }

  return (
    <div className="flex gap-2.5">
      <RequesterAvatar name={requester} />
      <div className="flex-1 max-w-[70%]">
        <div className="text-[11px] text-feishu-muted mb-1 ml-1">@{requester} 提问</div>
        <div className="px-3.5 py-2.5 rounded-2xl rounded-tl-md bg-white border border-feishu-border text-feishu-text text-sm">
          <div className="whitespace-pre-wrap break-words">{question.question}</div>

          {pending ? (
            <div className="mt-2.5">
              {question.question_type === "input" ? (
                <div className="flex gap-2 items-end">
                  <textarea
                    className="flex-1 resize-none border border-feishu-border rounded-lg px-2.5 py-1.5 text-sm outline-none focus:border-feishu-primary"
                    rows={2}
                    placeholder="输入答复"
                    value={text}
                    onChange={(event) => {
                      setText(event.target.value);
                      setError(null);
                    }}
                  />
                  <button
                    type="button"
                    onClick={submitInput}
                    className="px-3 py-1.5 rounded-lg bg-feishu-primary text-white text-[13px] font-medium hover:bg-feishu-primary-dark"
                  >
                    提交
                  </button>
                </div>
              ) : null}

              {question.question_type === "select" ? (
                <div className="flex flex-col gap-1.5">
                  {question.options.map((option) => {
                    const checked = selected.includes(option);
                    return (
                      <label
                        key={option}
                        className="flex items-center gap-2 cursor-pointer text-[13px]"
                      >
                        <input
                          type={question.multi ? "checkbox" : "radio"}
                          checked={checked}
                          onChange={() => toggleOption(option)}
                          className="accent-feishu-primary"
                        />
                        {option}
                      </label>
                    );
                  })}
                  <button
                    type="button"
                    onClick={submitSelect}
                    className="self-start px-3 py-1.5 rounded-lg bg-feishu-primary text-white text-[13px] font-medium hover:bg-feishu-primary-dark"
                  >
                    提交
                  </button>
                </div>
              ) : null}

              {question.question_type === "confirm" ? (
                <div className="flex gap-2">
                  {question.kind === "confirm_cancel" ? (
                    <>
                      <button
                        type="button"
                        onClick={() => onAnswer(question, true, "打断")}
                        className="px-3 py-1.5 rounded-lg bg-feishu-danger text-white text-[13px] font-medium"
                      >
                        打断
                      </button>
                      <button
                        type="button"
                        onClick={() => onAnswer(question, false, "保留")}
                        className="px-3 py-1.5 rounded-lg border border-feishu-border text-[13px]"
                      >
                        保留
                      </button>
                    </>
                  ) : (
                    <>
                      <button
                        type="button"
                        onClick={() => onAnswer(question, true, "确认")}
                        className="px-3 py-1.5 rounded-lg bg-feishu-primary text-white text-[13px] font-medium hover:bg-feishu-primary-dark"
                      >
                        确认
                      </button>
                      <button
                        type="button"
                        onClick={() => onAnswer(question, false, "取消")}
                        className="px-3 py-1.5 rounded-lg border border-feishu-border text-[13px]"
                      >
                        取消
                      </button>
                    </>
                  )}
                </div>
              ) : null}

              {error ? <div className="mt-1 text-[12px] text-feishu-danger">{error}</div> : null}
            </div>
          ) : (
            <div className="mt-2 text-[12px] text-feishu-muted">
              {question.status === "resolved" ? `已答：${answerLabel(question)}` : null}
              {question.status === "expired" ? "该问题已无需处理" : null}
              {question.status === "rejected" ? `答复被拒绝：${question.error ?? ""}` : null}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
