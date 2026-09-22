import { describe, expect, it } from "vitest";

import { splitMentions } from "./mentions";

describe("splitMentions", () => {
  it("splits @names into mention segments", () => {
    expect(splitMentions("@product-manager 请看下 @qa-engineer 的报告")).toEqual([
      { kind: "mention", value: "@product-manager" },
      { kind: "text", value: " 请看下 " },
      { kind: "mention", value: "@qa-engineer" },
      { kind: "text", value: " 的报告" },
    ]);
  });

  it("does not match emails or mid-word @", () => {
    expect(splitMentions("联系 user@product-manager 确认")).toEqual([
      { kind: "text", value: "联系 user@product-manager 确认" },
    ]);
  });

  it("matches after chinese characters", () => {
    const segments = splitMentions("麻烦@developer 支持一下");
    expect(segments.some((s) => s.kind === "mention" && s.value === "@developer")).toBe(true);
  });

  it("matches hyphen and underscore names", () => {
    const segments = splitMentions("@code_reviewer-2 ok");
    expect(segments[0]).toEqual({ kind: "mention", value: "@code_reviewer-2" });
  });

  it("keeps plain text as one segment", () => {
    expect(splitMentions("普通消息，没有提及")).toEqual([
      { kind: "text", value: "普通消息，没有提及" },
    ]);
  });

  it("handles empty text", () => {
    expect(splitMentions("")).toEqual([]);
  });
});
