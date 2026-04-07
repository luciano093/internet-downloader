import { Search } from "lucide-react";

export default function TopBarSearch() {
  return (
    <div className="relative flex w-full max-w-md items-center justify-center">
      <Search className="absolute left-3 h-4 w-4 text-muted" />
      <input
        type="search"
        placeholder="Search"
        className="h-8 w-full pt-[1px] rounded-sm bg-header-search border border-[#2A2D30] pl-9 pr-14 text-[13px] text-foreground placeholder:text-muted focus:border-gray-600 focus:outline-none "
      />
      <div className="absolute right-2 flex items-center">
        <kbd className="hidden rounded border border-chart-4 bg-header-search px-1.5 py-0.5 text-[10px] font-medium text-chart-2 sm:inline-block">
          Ctrl+K
        </kbd>
      </div>
    </div>
  );
}