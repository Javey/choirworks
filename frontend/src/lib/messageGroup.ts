import type { RoomMessageDto } from "./types";

import { formatTimeDivider } from "../components/TimeDivider";

export interface MessageGroupItem {
  kind: "divider";
  key: string;
  text: string;
}

export interface MessageGroupMessage {
  kind: "message";
  key: string;
  message: RoomMessageDto;
  grouped: boolean;
  showTime: boolean;
}

export type TimelineItem = MessageGroupItem | MessageGroupMessage;

const GROUP_INTERVAL_MS = 5 * 60 * 1000;
const DIVIDER_INTERVAL_MS = 30 * 60 * 1000;

function senderKey(message: RoomMessageDto): string {
  return `${message.role}:${message.sender ?? ""}`;
}

function timeOf(message: RoomMessageDto): number {
  return message.created_at ? new Date(message.created_at).getTime() : 0;
}

export function groupMessages(
  messages: RoomMessageDto[],
): TimelineItem[] {
  const items: TimelineItem[] = [];

  for (let i = 0; i < messages.length; i++) {
    const msg = messages[i];
    const prev = i > 0 ? messages[i - 1] : null;
    const prevTime = prev ? timeOf(prev) : 0;
    const currTime = timeOf(msg);

    if (prev) {
      const timeDiff = currTime - prevTime;
      const sameSender = senderKey(prev) === senderKey(msg);
      const needsDivider = timeDiff > DIVIDER_INTERVAL_MS || !sameSender;

      if (needsDivider && currTime > 0) {
        items.push({
          kind: "divider",
          key: `divider-${msg.id}`,
          text: formatTimeDivider(msg.created_at),
        });
      }
    } else if (currTime > 0) {
      items.push({
        kind: "divider",
        key: `divider-${msg.id}`,
        text: formatTimeDivider(msg.created_at),
      });
    }

    const grouped = prev
      ? senderKey(prev) === senderKey(msg) &&
        currTime - prevTime < GROUP_INTERVAL_MS
      : false;

    items.push({
      kind: "message",
      key: `msg-${msg.id}`,
      message: msg,
      grouped,
      showTime: !grouped,
    });
  }

  return items;
}
