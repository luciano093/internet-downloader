import { cn } from "@/lib/utils";
import type { LucideIcon } from "lucide-react";

interface SidebarItemProps {
  icon: LucideIcon;
  label: string;
  badge?: number | string;
  isActive?: boolean;
  onClick?: () => void;
}

export function SidebarItem({ 
  icon: Icon, 
  label, 
  badge, 
  isActive, 
  onClick 
}: SidebarItemProps) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "w-full flex items-center gap-2 px-2 py-0.5 text-[13px] cursor-pointer transition-colors text-muted-foreground hover:bg-accent hover:text-accent-foreground",
        
        isActive && "bg-accent text-accent-foreground",
      )}
    >
      {/* 1. Icon */}
      <Icon className="w-4 h-4 ml-1 opacity-70" />
      
      {/* 2. Label */}
      <span className="flex-1 text-left truncate">{label}</span>
      
      {/* 3. Optional Badge */}
      {badge !== undefined && (
        <span className="text-xs opacity-50 ml-auto">{badge}</span>
      )}
    </button>
  );
}