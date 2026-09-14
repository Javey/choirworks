const AVATAR_COLORS = [
  "#3370ff",
  "#7c3aed",
  "#34c759",
  "#ff9500",
  "#f54a45",
  "#00b894",
  "#e17055",
  "#6c5ce7",
  "#0984e3",
  "#e84393",
];

function colorFromName(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++) {
    hash = name.charCodeAt(i) + ((hash << 5) - hash);
  }
  return AVATAR_COLORS[Math.abs(hash) % AVATAR_COLORS.length];
}

export function Avatar({
  name,
  size = 36,
  bg,
}: {
  name: string;
  size?: number;
  bg?: string;
}) {
  const initial = name.slice(0, 1).toUpperCase();
  const background = bg ?? colorFromName(name);
  return (
    <div
      className="flex shrink-0 items-center justify-center rounded-full font-semibold text-white select-none"
      style={{
        width: size,
        height: size,
        background,
        fontSize: size * 0.4,
      }}
    >
      {initial}
    </div>
  );
}
