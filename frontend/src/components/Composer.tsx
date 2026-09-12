import { useState } from "react";

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
    if (!value || disabled) return;
    void onSend(value);
    setText("");
  };

  return (
    <div className="composer">
      <textarea
        className="composer-input"
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
        className="button primary"
        disabled={disabled || !text.trim()}
        onClick={submit}
      >
        发送
      </button>
    </div>
  );
}
