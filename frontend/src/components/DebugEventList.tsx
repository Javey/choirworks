import { useEffect, useRef, useState } from "react";
import { Check, Copy } from "lucide-react";

import { coreToWire } from "../lib/coreToWire";

export function DebugEventList({ events }: { events: unknown[] }) {
  const bottomRef = useRef<HTMLDivElement>(null);
  const [expanded, setExpanded] = useState<Set<number>>(new Set());
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [events.length]);

  const toggle = (index: number) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(index)) {
        next.delete(index);
      } else {
        next.add(index);
      }
      return next;
    });
  };

  async function copyAll() {
    const text = events
      .map((event, i) => {
        const wire = coreToWire(event) as Record<string, unknown>;
        return `// --- event #${i} ---\n${JSON.stringify(wire, null, 2)}`;
      })
      .join("\n\n");
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }

  return (
    <div className="flex-1 min-h-0 overflow-y-auto bg-gray-50 px-4 py-2">
      {events.length === 0 ? (
        <div className="flex items-center justify-center h-full text-sm text-gray-400">
          等待事件…
        </div>
      ) : (
        <div className="flex flex-col gap-1.5">
          <div className="sticky top-0 flex justify-end -mx-4 -mt-2 px-4 py-1 bg-gray-50/90 backdrop-blur-sm z-10">
            <button
              type="button"
              onClick={() => void copyAll()}
              className="flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700 transition-colors"
            >
              {copied ? (
                <>
                  <Check size={12} />
                  已复制
                </>
              ) : (
                <>
                  <Copy size={12} />
                  复制全部
                </>
              )}
            </button>
          </div>
          {events.map((event, index) => {
            const wire = coreToWire(event) as Record<string, unknown>;
            const payloadType = Object.keys(wire)[0] ?? "unknown";
            const payloadValue = (wire[payloadType] as Record<string, unknown>) ?? {};
            const metadata =
              (payloadValue.metadata as Record<string, unknown> | undefined) ??
              (payloadValue.status as Record<string, unknown> | undefined)?.metadata as
                | Record<string, unknown>
                | undefined;
            const kind =
              typeof metadata?.kind === "string" ? metadata.kind : "";
            const isOpen = expanded.has(index);

            return (
              <div
                key={index}
                className="rounded-lg border border-gray-200 bg-white overflow-hidden"
              >
                <button
                  type="button"
                  onClick={() => toggle(index)}
                  className="flex items-center gap-2 w-full px-3 py-1.5 bg-gray-100 border-b border-gray-200 hover:bg-gray-150 transition-colors cursor-pointer"
                >
                  <span className="text-xs text-gray-400">
                    {isOpen ? "▾" : "▸"}
                  </span>
                  <span className="text-xs font-mono text-gray-400">
                    #{index}
                  </span>
                  <span className="text-xs font-semibold text-gray-700">
                    {payloadType}
                  </span>
                  {kind ? (
                    <span className="text-xs font-mono text-blue-600">
                      {kind}
                    </span>
                  ) : null}
                </button>
                {isOpen ? (
                  <pre className="px-3 py-2 text-xs font-mono text-gray-800 overflow-x-auto whitespace-pre-wrap break-all max-h-96 overflow-y-auto">
                    {JSON.stringify(wire, null, 2)}
                  </pre>
                ) : null}
              </div>
            );
          })}
          <div ref={bottomRef} />
        </div>
      )}
    </div>
  );
}
