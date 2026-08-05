import { formatBytes } from "@/routes/downloads/components/DownloadsTable";

export function formatLimit(bytes: number | null): string {
  if (bytes === null) return "No Limit";
  if (bytes === 0) return "0 B/s";
  return `${formatBytes(bytes)}/s`
}

export function mbToBytes(mb: number | null): number | null {
  if (mb == null) {
    return null
  } else {
    return Math.round(mb * 1024 * 1024);
  }
}

export function bytesToMb(bytes: number | null): number | null {
  if (bytes == null) {
    return null
  } else {
    return Math.round(bytes / 1024 / 1024);
  }
}
