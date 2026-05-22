import type { DownloadStatus } from "@/downloadTypes";
import { type LucideIcon, ArrowDownToLine, Pause, Check, X } from "lucide-react";

export type FilterCategory = "active" | "paused" | "completed" | "failed";

export const STATE_TO_CATEGORY: Record<DownloadStatus["state"], FilterCategory> = {
  queued: "active",
  initializing: "active",
  fetching_metadata: "active",
  in_progress: "active",
  retrying: "active",
  waiting: "active",
  paused: "paused",
  completed: "completed",
  completed_with_errors: "completed",
  failed: "failed",
  not_found: "failed",
};

export const STATUS_FILTERS: { id: FilterCategory; label: string; icon: LucideIcon }[] = [
  { id: "active", label: "Downloading", icon: ArrowDownToLine },
  { id: "paused", label: "Paused", icon: Pause },
  { id: "completed", label: "Completed", icon: Check },
  { id: "failed", label: "Failed", icon: X },
];
