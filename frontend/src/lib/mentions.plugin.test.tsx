import { render } from "@testing-library/react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { describe, expect, it } from "vitest";

import { remarkMention } from "./mentions";

// Regression: the plugin must be a unified attacher (factory returning a
// transformer); passing a bare transformer crashed react-markdown at parse.
describe("remarkMention plugin", () => {
  it("renders mentions as member anchors, preserving inline code", () => {
    const { container } = render(
      <ReactMarkdown remarkPlugins={[remarkGfm, remarkMention]}>
        {"@product-manager 请查收；代码 `@developer` 不受影响"}
      </ReactMarkdown>,
    );
    const anchors = container.querySelectorAll('a[href="#member-product-manager"]');
    expect(anchors).toHaveLength(1);
    expect(anchors[0].textContent).toBe("@product-manager");
    const code = container.querySelector("code");
    expect(code?.textContent).toBe("@developer");
  });

  it("leaves plain text untouched when there are no mentions", () => {
    const { container } = render(
      <ReactMarkdown remarkPlugins={[remarkGfm, remarkMention]}>
        {"普通消息，没有提及"}
      </ReactMarkdown>,
    );
    expect(container.querySelectorAll('a[href^="#member-"]')).toHaveLength(0);
    expect(container.textContent).toContain("普通消息，没有提及");
  });
});
