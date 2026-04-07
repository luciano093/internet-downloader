import { SidebarGroup } from "@/components/SidebarGroup";
import { SidebarItem } from "@/components/SidebarItem";
import { List, ArrowDownToLine, Pause, Check, HardDrive } from "lucide-react";

export default function DownloadsSidebar() {
  return (
    <div className="flex flex-col gap-0">
      {/* STATUS Section */}
      <SidebarGroup title="Status">
        <SidebarItem icon={List} label="All" badge={4} isActive={true} />
        <SidebarItem icon={ArrowDownToLine} label="Downloading" badge={2} />
        <SidebarItem icon={Pause} label="Paused" badge={1} />
        <SidebarItem icon={Check} label="Completed" badge={1} />
      </SidebarGroup>

      {/* HOSTS Section */}
      <SidebarGroup title="Hosts">
        <SidebarItem icon={HardDrive} label="releases.ubuntu.com" badge={1} />
        <SidebarItem icon={HardDrive} label="github.com" badge={1} />
      </SidebarGroup>
    </div>
  )
}