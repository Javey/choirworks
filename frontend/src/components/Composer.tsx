import { useState } from "react";
import { CornerDownLeft } from "lucide-react";

export function Composer({
  disabled,
  hint,
  onSend,
}: {
  disabled: boolean;
  hint: string;
  onSend: (text: string) => void | Promise<void>;
}) {
  const [text, setText] = useState("");

  const submit = () => {
    const value = text.trim();
    if (value || disabled) {
      void onSend(value);
    }
    if (!disabled) setText("");
  };

  return (
    <div className="flex gap-2.5 items-end px-6 py-3 bg-white border-t border-feishu-border flex-shrink-0">
      <textarea
        className="flex-1 resize-none border border-feishu-border rounded-xl px-3.5 py-2.5 text-sm font-sans outline-none focus:border-feishu-primary focus:ring-2 focus:ring-feishu-primary/10 transition-all bg-feishu-bg/50 placeholder:text-feishu-muted"
        rows={3}
        placeholder={disabled ? hint : "输入消息，Enter 发送，Shift+Enter 换行"}
        value={text}
        disabled={disabled}
        onChange={(event) => setText(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            submit();
          }
        }}
      />
      <button
        type="button"
        disabled={disabled || !text.trim()}
        onClick={submit}
        className="flex items-center gap-1.5 px-4 py-2.5 rounded-xl bg-feishu-primary text-white text-[13px] font-medium hover:bg-feishu-primary-dark disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
      >
        发送
        <CornerDownLeft size={14} />
      </button>
    </div>
  );
}
