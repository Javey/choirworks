import { memo } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import type { ReactNode } from "react";

function highlightMentionsInNode(
  node: ReactNode,
  memberNames: Set<string>,
): ReactNode {
  if (typeof node === "string") {
    const parts = node.split(/(@[A-Za-z0-9_-]+)/g);
    if (parts.length === 1) return node;
    return parts.map((part, index) => {
      const name = part.startsWith("@") ? part.slice(1) : null;
      if (name && memberNames.has(name)) {
        return (
          <span key={index} className="mention">
            {part}
          </span>
        );
      }
      return <span key={index}>{part}</span>;
    });
  }
  if (Array.isArray(node)) {
    return node.map((child) => highlightMentionsInNode(child, memberNames));
  }
  return node;
}

export const Markdown = memo(function Markdown({
  children,
  memberNames,
}: {
  children: string;
  memberNames?: Set<string>;
}) {
  const wrap = (node: ReactNode) =>
    memberNames ? highlightMentionsInNode(node, memberNames) : node;

  return (
    <div className="text-sm leading-relaxed break-words">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          p: ({ children }) => <p className="m-0 first:mt-0 last:mb-0">{wrap(children)}</p>,
          ul: ({ children }) => <ul className="m-0 pl-5 list-disc space-y-1">{children}</ul>,
          ol: ({ children }) => <ol className="m-0 pl-5 list-decimal space-y-1">{children}</ol>,
          li: ({ children }) => <li className="leading-relaxed">{wrap(children)}</li>,
          code: ({ children, className }) => {
            const isInline = !className?.includes("language-");
            if (isInline) {
              return (
                <code className="px-1.5 py-0.5 rounded bg-feishu-bg text-feishu-primary text-[13px] font-mono">
                  {children}
                </code>
              );
            }
            return (
              <code className="block font-mono text-[13px]">{children}</code>
            );
          },
          pre: ({ children }) => (
            <pre className="my-2 p-3 rounded-lg bg-[#1e1e2e] text-[#cdd6f4] overflow-x-auto text-[13px] leading-relaxed">
              {children}
            </pre>
          ),
          a: ({ children, href }) => (
            <a
              href={href}
              target="_blank"
              rel="noopener noreferrer"
              className="text-feishu-primary hover:underline"
            >
              {children}
            </a>
          ),
          blockquote: ({ children }) => (
            <blockquote className="my-2 pl-3 border-l-[3px] border-feishu-primary-border text-feishu-text-secondary">
              {children}
            </blockquote>
          ),
          h1: ({ children }) => <h1 className="text-lg font-bold mt-3 mb-2 first:mt-0">{children}</h1>,
          h2: ({ children }) => <h2 className="text-base font-bold mt-3 mb-2 first:mt-0">{children}</h2>,
          h3: ({ children }) => <h3 className="text-sm font-bold mt-2 mb-1 first:mt-0">{children}</h3>,
          hr: () => <hr className="my-3 border-feishu-border" />,
          table: ({ children }) => (
            <div className="my-2 overflow-x-auto">
              <table className="min-w-full border-collapse text-xs">{children}</table>
            </div>
          ),
          th: ({ children }) => (
            <th className="border border-feishu-border px-2 py-1 bg-feishu-bg font-semibold text-left">
              {children}
            </th>
          ),
          td: ({ children }) => (
            <td className="border border-feishu-border px-2 py-1">{children}</td>
          ),
        }}
      >
        {children}
      </ReactMarkdown>
    </div>
  );
});

export function highlightMentions(
  text: string,
  memberNames: Set<string>,
): ReactNode[] {
  const parts = text.split(/(@[A-Za-z0-9_-]+)/g);
  return parts.map((part, index) => {
    const name = part.startsWith("@") ? part.slice(1) : null;
    if (name && memberNames.has(name)) {
      return (
        <span key={index} className="mention">
          {part}
        </span>
      );
    }
    return <span key={index}>{part}</span>;
  });
}
