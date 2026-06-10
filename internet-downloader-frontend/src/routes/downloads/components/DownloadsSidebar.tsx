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
  const hostFilter = useDownloadStore(store => store.hostFilter);
  const setHostFilter = useDownloadStore(store => store.setHostFilter);
  
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
        {/* entry2[1] - entry1[1] sorts cost entries by count, descending. An entry has this shape: [host, count] */}
        {Object.entries(counts.hosts).sort((entry1, entry2) => entry2[1] - entry1[1]).map(([host, count]) => (
          <SidebarItem
            key={host}
            icon={HardDrive}
            label={host}
            badge={count}
            isActive={hostFilter === host}
            onClick={() => setHostFilter(hostFilter === host ? null : host)}
          />
        ))}
      </SidebarGroup>
    </div>
  )
}
