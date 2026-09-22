import type { Link, Root, RootContent, Text } from "mdast";

export interface MentionSegment {
  kind: "text" | "mention";
  value: string;
}

// Mentions are ASCII agent ids, same pattern the backend uses for
// arbitration. The lookbehind skips emails ("user@name") and mid-word "@".
const MENTION_PATTERN = /(?<![\w@])@([A-Za-z0-9_-]+)/g;

export function splitMentions(text: string): MentionSegment[] {
  const segments: MentionSegment[] = [];
  let last = 0;
  for (const match of text.matchAll(MENTION_PATTERN)) {
    const index = match.index ?? 0;
    if (index > last) {
      segments.push({ kind: "text", value: text.slice(last, index) });
    }
    segments.push({ kind: "mention", value: match[0] });
    last = index + match[0].length;
  }
  if (last < text.length) {
    segments.push({ kind: "text", value: text.slice(last) });
  }
  return segments;
}

const memberAnchor = (name: string): string => `#member-${name}`;

export function isMemberAnchor(href: string | undefined | null): boolean {
  return typeof href === "string" && href.startsWith("#member-");
}

/**
 * remark plugin turning `@name` text nodes into member anchor links so
 * react-markdown can style them. Code spans/fences are untouched because
 * only `text` nodes are rewritten.
 */
export function remarkMention(): (tree: Root) => void {
  return (tree: Root): void => {
    walk(tree.children, false);
  };
}

function walk(nodes: RootContent[], inLink: boolean): void {
  for (let i = nodes.length - 1; i >= 0; i -= 1) {
    const child = nodes[i];
    if (child.type === "text") {
      if (inLink) continue;
      const segments = splitMentions((child as Text).value);
      if (segments.every((segment) => segment.kind === "text")) continue;
      const replacements: RootContent[] = segments.map((segment) => {
        if (segment.kind === "text") {
          const node: Text = { type: "text", value: segment.value };
          return node;
        }
        const link: Link = {
          type: "link",
          url: memberAnchor(segment.value.slice(1)),
          title: null,
          children: [{ type: "text", value: segment.value }],
        };
        return link;
      });
      nodes.splice(i, 1, ...replacements);
      continue;
    }
    if ("children" in child && Array.isArray(child.children)) {
      walk(child.children, inLink || child.type === "link");
    }
  }
}
