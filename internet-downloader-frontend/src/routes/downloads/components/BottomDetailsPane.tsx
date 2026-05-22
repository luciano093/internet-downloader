import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatDownloadStatus } from "@/lib/status_utils";
import { useDownloadStore } from "@/stores/downloadStore";
import type { ReactNode } from "react";
import { formatBytes, getFolderStats } from "./DownloadsTable";
import FileTree from "./FileTree";

function DetailsPaneTab({ value, children }: { value: string, children: ReactNode }) {
    return <>
        <TabsTrigger 
            value={value} 
            className="rounded-none border-0 border-r border-b border-border px-4 text-[11px] tracking-wider uppercase dark:text-muted dark:data-[state=active]:bg-background dark:data-[state=active]:text-foreground shadow-none"
        >
            {children}
        </TabsTrigger>
    </>
}

export default function BottomDetailsPane() {
    const selectedId = useDownloadStore((state) => state.selectedId);

    if (!selectedId) return null;

    const download = useDownloadStore((state) => state.downloads[selectedId]);

    if (!download) return null;

    return (
        <div className="w-full h-full bg-background flex flex-col -mt-[2px]">
            <Tabs defaultValue="general" className="flex flex-col h-full gap-0">

                {/* Tabs */}
                <div className="w-full bg-sidebar border-b border-border h-8">
                    <TabsList className="flex justify-start rounded-none bg-transparent p-0 w-fit">
                        <DetailsPaneTab value="general">
                            General
                        </DetailsPaneTab>
                        
                        <DetailsPaneTab value="files">
                            Files
                        </DetailsPaneTab>

                        <DetailsPaneTab value="output">
                            {`>_ Output/Logs`}
                        </DetailsPaneTab>
                    </TabsList>
                </div>

                {/* Tab Content */}
                <TabsContent value="general" className="flex-1 p-6 m-0 overflow-auto focus-visible:outline-none">
                    <div className="grid grid-cols-[120px_1fr] gap-y-3 text-[13px]">
                        
                        <div className="text-right pr-6 text-foreground/50 font-medium">Name:</div>
                        <div className="text-foreground truncate">{download.name || "Unknown"}</div>

                        <div className="text-right pr-6 text-foreground/50 font-medium">Status:</div>
                        <div className="text-foreground">{formatDownloadStatus(download.status)}</div>

                        <div className="text-right pr-6 text-foreground/50 font-medium">Size:</div>
                        <div className="text-foreground">{formatBytes(getFolderStats(download.files).totalSize)}</div>
                        
                        <div className="text-right pr-6 text-foreground/50 font-medium">Save Path:</div>
                        <div className="text-foreground truncate">/downloads/completed/</div>

                    </div>

                </TabsContent>
                <TabsContent value="files" className="flex-1 p-0 m-0 overflow-auto">
                  <FileTree download={download} />
                </TabsContent>
                
                <TabsContent value="output" className="flex-1 p-4 m-0 overflow-auto font-mono text-xs text-muted">
                    [System] Download initialized.
                </TabsContent>

            </Tabs>
        </div>
    );
}
