import { Plus, Play, Pause, X, Globe, Download } from "lucide-react";
import { TopBarButton } from "@/components/TopBarButton";
import TopBarSearch from "@/components/TopBarSearch";
import { useUiStore } from "@/stores/uiStore";
import { useDownloadStore } from "@/stores/downloadStore";
import { useMutation } from "@tanstack/react-query";
import { useSettings } from "@/stores/settingsStore";

export default function DownloadsTopBar({ aggregateSpeed }: { aggregateSpeed: number }) {
    const openModal = useUiStore((state) => state.openModal);
    const selectedId = useDownloadStore((state) => state.selectedId);
    const { data: settings } = useSettings();
  
    const globalSpeedLimit = settings?.global_speed_limit ?? null;

    const globalSpeedLimitMbs = globalSpeedLimit ? (globalSpeedLimit / (1024 * 1024)).toFixed(2) : null;
  
    const speedMbs = aggregateSpeed > 0 ? (aggregateSpeed / (1024 * 1024)).toFixed(2) : null;
  
    const pauseMutation = useMutation({
        mutationFn: async (id: number) => {
            return fetch(`http://localhost:3211/downloads/${id}/pause`, {
                method: "POST",
            });
        },
    });

    const resumeMutation = useMutation({
        mutationFn: async (id: number) => {
            return fetch(`http://localhost:3211/downloads/${id}/resume`, {
                method: "POST",
            });
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
            disabled={selectedId === null || (resumeMutation.isPending && resumeMutation.variables === selectedId)}
            onClick={() => {
                    if (selectedId !== null) {
                        resumeMutation.mutate(selectedId);
                    }
                }}
            />

            <TopBarButton 
            icon={<Pause className="h-4 w-4"/>} 
            label="Pause"
            disabled={selectedId === null || (pauseMutation.isPending && pauseMutation.variables === selectedId)}
            onClick={() => {
                    if (selectedId !== null) {
                        pauseMutation.mutate(selectedId);
                    }
                }}
            />

            <div className="h-5 w-px bg-gray-700 mx-1" /> 
            
            <TopBarButton 
            icon={<X className="h-4 w-4"/>} 
            label="Remove"
            disabled={selectedId === null}
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
            <div className="flex items-center gap-2">
            <Globe className="h-4 w-4 text-gray-500" />
            <span>{globalSpeedLimitMbs || "No Limit"}</span>
            </div>
        </div>

      </div>
    );
}
