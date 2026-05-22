import { SidebarGroup } from "@/components/SidebarGroup";
import { SidebarItem } from "@/components/SidebarItem";
import { List, HardDrive } from "lucide-react";
import { useDownloadCounts } from "../hooks/useDownloadCounts";
import { useDownloadStore } from "@/stores/downloadStore";
import { STATUS_FILTERS } from "../lib/filters";

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
