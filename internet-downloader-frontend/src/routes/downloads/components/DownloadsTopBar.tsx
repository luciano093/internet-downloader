import { Plus, Play, Pause, X, ArrowDown, Globe } from "lucide-react";
import { TopBarButton } from "@/components/TopBarButton";
import TopBarSearch from "@/components/TopBarSearch";
import { useUiStore } from "@/stores/uiStore";
import { useDownloadStore } from "@/stores/downloadStore";
import { useMutation } from "@tanstack/react-query";

export default function DownloadsTopBar() {
    const openModal = useUiStore((state) => state.openModal);
    const selectedId = useDownloadStore((state) => state.selectedId);

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
        <div className="flex w-full items-center h-full">
        
        {/* Buttons */}
        <div className="flex items-center h-full">
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
        <div className="flex flex-1 items-center justify-center h-full">
            <TopBarSearch />
        </div>

        {/* Stats */}
        <div className="flex items-center gap-6 text-[13px] text-gray-400">
            <div className="flex items-center gap-2">
            <ArrowDown className="h-4 w-4 text-blue-500" />
            <span>16 MB/s</span>
            </div>
            <div className="flex items-center gap-2">
            <Globe className="h-4 w-4 text-gray-500" />
            <span>No Limit</span>
            </div>
        </div>

        </div>
    );
}