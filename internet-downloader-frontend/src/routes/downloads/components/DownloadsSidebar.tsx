import { SidebarGroup } from "@/components/SidebarGroup";
import { SidebarItem } from "@/components/SidebarItem";
import type { DownloadStatus } from "@/downloadTypes";
import { List, ArrowDownToLine, Pause, Check, HardDrive, type LucideIcon, X } from "lucide-react";
import { useDownloadCounts } from "../hooks/useDownloadCounts";
import { useDownloadStore } from "@/stores/downloadStore";

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

const STATUS_FILTERS: { id: FilterCategory; label: string; icon: LucideIcon }[] =[
  { id: "active", label: "Downloading", icon: ArrowDownToLine },
  { id: "paused", label: "Paused", icon: Pause },
  { id: "completed", label: "Completed", icon: Check },
  { id: "failed", label: "Failed", icon: X },
];

export default function DownloadsSidebar() {
  const counts = useDownloadCounts();
  const statusFilter = useDownloadStore(store => store.statusFilter);
  const setStatusFilter = useDownloadStore(store => store.setStatusFilter);
  
  return (
    <div className="flex flex-col gap-0">
      {/* STATUS Section */}
      <SidebarGroup title="Status">
        {/* The "All" status is hardcoded */}
        <SidebarItem icon={List} label="All" badge={counts.all} isActive={statusFilter === null} onClick={() => setStatusFilter(null)} />

        {/* Rest of the statuses */}
        {
          STATUS_FILTERS.map((filter) => (
            <SidebarItem 
              key={filter.id}
              icon={filter.icon} 
              label={filter.label} 
              badge={counts.status[filter.id]} 
              isActive={statusFilter === filter.id}
              onClick={() => setStatusFilter(filter.id)}
            />
          ))}
      </SidebarGroup>

      {/* HOSTS Section */}
      <SidebarGroup title="Hosts">
        <SidebarItem icon={HardDrive} label="releases.ubuntu.com" badge={1} />
        <SidebarItem icon={HardDrive} label="github.com" badge={1} />
      </SidebarGroup>
    </div>
  )
}
