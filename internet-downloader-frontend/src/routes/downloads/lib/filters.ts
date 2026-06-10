import type { DownloadStatus } from "@/downloadTypes";
import { type LucideIcon, ArrowDownToLine, Pause, Check, X } from "lucide-react";

export type FilterCategory = "active" | "paused" | "completed" | "failed";

export const STATE_TO_CATEGORY: Record<DownloadStatus["state"], FilterCategory> = {
  uninitialized: "active",
  metadata_fetched: "active",
  partial: "active",
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

export function getFilterCategory(status: DownloadStatus, is_paused: boolean): FilterCategory {
  // active_operation overrides status for transient states
  if (is_paused) return "paused";
  return STATE_TO_CATEGORY[status.state];
}
