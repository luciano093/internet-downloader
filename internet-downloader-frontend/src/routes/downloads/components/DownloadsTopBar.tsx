import { Plus, Play, Pause, X, Download } from "lucide-react";
import { TopBarButton } from "@/components/TopBarButton";
import TopBarSearch from "@/components/TopBarSearch";
import { useUiStore } from "@/stores/uiStore";
import { useDownloadDataStore } from "@/stores/downloadStore";
import { useMutation } from "@tanstack/react-query";
import GlobalSpeedLimit from "@/components/GlobalSpeedLimit";
import useSelection from "@/hooks/useSelection";

export default function DownloadsTopBar({ aggregateSpeed }: { aggregateSpeed: number }) {
  const openModal = useUiStore((state) => state.openModal);
  const selection = useDownloadDataStore((state) => state.selection);

  const selectedIds = useSelection(selection, (selection) => selection.getSelected());
  
  const speedMbs = aggregateSpeed > 0 ? (aggregateSpeed / (1024 * 1024)).toFixed(2) : null;
  
  const pauseMutation = useMutation({
    mutationFn: async (ids: number[]) => {
      const promises = ids.map(async (id) => {
        const response = await fetch(`http://localhost:3211/downloads/${id}/pause`, {
          method: "POST",
        });

        if (!response.ok) {
          throw new Error(`Failed to pause download ID ${id}`);
        }
  
        return response;
      });

      const results = await Promise.allSettled(promises);

      const failed = results.filter((r) => r.status === "rejected");
      if (failed.length > 0) {
        if (failed.length === 1) {
          throw new Error(`${failed.length} download failed to pause`);
        } else {
          throw new Error(`${failed.length} downloads failed to pause`);
        }
      }
  
      return results;
    },
  });

  const resumeMutation = useMutation({
    mutationFn: async (ids: number[]) => {
      const promises = ids.map(async (id) => {
        const response = await fetch(`http://localhost:3211/downloads/${id}/resume`, {
          method: "POST",
        });
  
        if (!response.ok) {
          throw new Error(`Failed to resume download ID ${id}`);
        }
  
        return response;
      });
  
      const results = await Promise.allSettled(promises);

      const failed = results.filter((r) => r.status === "rejected");
      if (failed.length > 0) {
        if (failed.length === 1) {
          throw new Error(`${failed.length} download failed to resume`);
        } else {
          throw new Error(`${failed.length} downloads failed to resume`);
        }
      }
  
      return results;
    },
  });

  return (
      <div className="flex w-full items-center h-full relative">
        
        {/* Buttons */}
        <div className="flex flex-1 items-center h-full">
            <TopBarButton 
            icon={<Plus className="h-4 w-4"/>} 
            label="Add" 
            onClick={() => openModal('add')}
            />
            <div className="h-5 w-px bg-gray-700 mx-1" /> 
            <TopBarButton 
            icon={<Play className="h-4 w-4"/>} 
            label="Start"
            disabled={selectedIds.length === 0 || (resumeMutation.isPending)}
            onClick={() => {
                if (selectedIds.length !== 0) {
                  resumeMutation.mutate(selectedIds);
                }
              }}
            />

            <TopBarButton 
            icon={<Pause className="h-4 w-4"/>} 
            label="Pause"
            disabled={selectedIds.length === 0 || (pauseMutation.isPending)}
            onClick={() => {
              if (selectedIds.length !== 0) {
                pauseMutation.mutate(selectedIds);
              }
            }}
            />

            <div className="h-5 w-px bg-gray-700 mx-1" /> 
            
            <TopBarButton 
            icon={<X className="h-4 w-4"/>} 
            label="Remove"
            disabled={selectedIds.length === 0}
            onClick={() => openModal('remove')}
            />
        </div>

        {/* Search Bar */}
        <div className="absolute inset-0 flex items-center justify-center h-full pointer-events-none">
          <div className="w-full max-w-md pointer-events-auto">
            <TopBarSearch />
          </div>
        </div>

        {/* Stats */}
        <div className="flex items-center gap-6 text-[13px] text-gray-400">
            <div className="flex items-center gap-2">
            <Download className="h-4 w-4 text-blue-500" />
            <span>{speedMbs ? `${speedMbs} MB/s` : "0 MB/s"}</span>
            </div>
            <GlobalSpeedLimit />
        </div>

      </div>
    );
}
