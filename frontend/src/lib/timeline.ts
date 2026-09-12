import { isTerminal, type InterventionView, type NodeView, type TaskView } from "./taskView";
import type { TaskStatus } from "./types";

export type ChatItem =
  | { kind: "user"; key: string; taskId: string; text: string; at: string }
  | {
      kind: "plan";
      key: string;
      taskId: string;
      rationale?: string;
      nodes: { name: string; agentName?: string }[];
    }
  | { kind: "node"; key: string; taskId: string; node: NodeView }
  | { kind: "intervention"; key: string; taskId: string; intervention: InterventionView }
  | { kind: "note"; key: string; taskId: string; level: "info" | "warn"; text: string }
  | {
      kind: "result";
      key: string;
      taskId: string;
      status: TaskStatus;
      text?: string;
      error?: string;
    };

function resultText(view: TaskView): string | undefined {
  const ordered = view.nodeOrder
    .map((id) => view.nodes.find((node) => node.id === id))
    .filter((node): node is NodeView => Boolean(node));
  for (let index = ordered.length - 1; index >= 0; index -= 1) {
    const text = ordered[index].outputText;
    if (text) return text;
  }
  return undefined;
}

function resultError(view: TaskView): string | undefined {
  const failed = view.nodes.find((node) => node.status === "failed" && node.error);
  if (failed?.error) return failed.error;
  const warn = [...view.notes].reverse().find((note) => note.level === "warn");
  return warn?.text;
}

export function buildTimeline(views: TaskView[]): ChatItem[] {
  const items: ChatItem[] = [];
  for (const view of views) {
    items.push({
      kind: "user",
      key: `${view.id}:user`,
      taskId: view.id,
      text: view.request,
      at: view.createdAt,
    });

    if (view.nodeOrder.length > 0) {
      items.push({
        kind: "plan",
        key: `${view.id}:plan`,
        taskId: view.id,
        rationale: view.rationale,
        nodes: view.nodeOrder
          .map((id) => view.nodes.find((node) => node.id === id))
          .filter((node): node is NodeView => Boolean(node))
          .map((node) => ({ name: node.name, agentName: node.agentName })),
      });
    }

    for (const id of view.nodeOrder) {
      const node = view.nodes.find((candidate) => candidate.id === id);
      if (node) {
        items.push({ kind: "node", key: `${view.id}:node:${id}`, taskId: view.id, node });
      }
    }

    for (const intervention of view.interventions) {
      items.push({
        kind: "intervention",
        key: `${view.id}:intervention:${intervention.id}`,
        taskId: view.id,
        intervention,
      });
    }

    for (const note of view.notes) {
      items.push({
        kind: "note",
        key: `${view.id}:note:${note.id}`,
        taskId: view.id,
        level: note.level,
        text: note.text,
      });
    }

    if (isTerminal(view.status)) {
      items.push({
        kind: "result",
        key: `${view.id}:result`,
        taskId: view.id,
        status: view.status,
        text: view.status === "completed" ? resultText(view) : undefined,
        error: view.status === "failed" ? resultError(view) : undefined,
      });
    }
  }
  return items;
}
