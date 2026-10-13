export function formatNumber(value) {
  if (value === null || value === undefined) return "–";
  return Number(value).toLocaleString();
}

export function formatAge(minutes) {
  if (minutes === null || minutes === undefined) return "–";
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${Math.round(minutes)} min ago`;
  const hours = minutes / 60;
  if (hours < 48) return `${hours.toFixed(1)} h ago`;
  return `${Math.round(hours / 24)} days ago`;
}

export function formatDuration(seconds) {
  if (seconds === null || seconds === undefined) return "–";
  if (seconds < 60) return `${seconds.toFixed(1)} s`;
  const total = Math.round(seconds);
  return `${Math.floor(total / 60)}m ${total % 60}s`;
}

export function formatTime(iso) {
  if (!iso) return "–";
  return new Date(iso).toLocaleString();
}
