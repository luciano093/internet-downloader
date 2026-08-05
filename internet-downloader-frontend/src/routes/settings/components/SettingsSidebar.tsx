import { SidebarGroup } from "@/components/SidebarGroup";
import { SidebarItem } from "@/components/SidebarItem";
import { Settings } from "lucide-react";

export default function SettingsSidebar() {
  return (
    <div className="flex flex-col gap-0">
      <SidebarGroup title="Preferences">
        <SidebarItem icon={Settings} label="All" isActive={true} onClick={() => {}} />
      </SidebarGroup>
    </div>
  );
}
