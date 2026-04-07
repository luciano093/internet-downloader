import { cn } from "@/lib/utils";
import { ChevronDown } from "lucide-react";
import { useState } from "react";

interface SidebarGroupProps {
  title: string;
  children: React.ReactNode;
  defaultExpanded?: boolean;
}

export function SidebarGroup({ title, children, defaultExpanded = true }: SidebarGroupProps) {
  const[isExpanded, setIsExpanded] = useState(defaultExpanded);

  return (
    <div className="flex flex-col">
      {/* Header */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="flex items-center gap-1 px-2 mb-1 w-full text-left cursor-pointer text-muted-foreground hover:text-foreground transition-colors"
      >
        <ChevronDown 
          className={cn(
            "w-3.5 h-3.5 transition-transform duration-200", 
            !isExpanded && "-rotate-90"
          )} 
        />
        <span className="text-[11px] font-semibold tracking-wider uppercase">
          {title}
        </span>
      </button>

      {/* List of Items*/}
      {isExpanded && (
        <div className="flex flex-col mb-4">
          {children}
        </div>
      )}
    </div>
  );
}