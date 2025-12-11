import { useDownloadStore } from "@/store";
import { memo, useMemo, useState } from "react";
import { TableCell, TableRow } from "./ui/table";
import type { DownloadNode, FileItem } from "@/downloadTypes";

import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Button } from "./ui/button";
import { Loader2, Trash2 } from "lucide-react";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";

function formatBytes(bytes: number, decimals = 2) {
    if (!+bytes) return '0 B';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
}

    
function getFolderStats(files: Record<number, DownloadNode>) {
    const allItems = Object.values(files);
    const activeFiles = allItems.filter((item): item is FileItem => item.type === 'file');

    if (activeFiles.length === 0) {
        return { progress: 0, totalSize: 0, downloadedSize: 0 };
    }

    let totalBytes = 0;
    let downloadedBytes = 0;

    activeFiles.forEach(file => {
        const size = typeof file.size === 'number' ? file.size : 0;
        const downloaded = file.bytes_downloaded;

        totalBytes += size;
        downloadedBytes += downloaded;
    });

    const percentage = totalBytes === 0 ? 0 : (downloadedBytes / totalBytes) * 100;

    return {
        progress: percentage,
        totalSize: totalBytes,
        downloadedSize: downloadedBytes
    };
}

export const DownloadRow = memo(({ id }: { id: number }) => {
    const download = useDownloadStore((state) => state.downloads[id]);

    const [deleteFromDisk, setDeleteFromDisk] = useState(false);
    const [isDeleting, setIsDeleting] = useState(false);

    console.log(JSON.parse(JSON.stringify(download)));
    console.log(`Row ${id} Render:`, download?.status, download?.url);

    const { progress, totalSize, downloadedSize } = useMemo(() => {
        if (!download || !download.files || !download.files[0]) {
            return { progress: 0, totalSize: 0, downloadedSize: 0 };
        }

        const rootNode = download.files[0];

        if (rootNode.type === 'file') {
            const current = rootNode.bytes_downloaded || 0;
            const total = typeof rootNode.size === 'number' ? rootNode.size : 0;

            return { 
                progress: total === 0 ? 0 : (current / total) * 100,
                totalSize: total,
                downloadedSize: current
            };
        }
        
        return getFolderStats(download.files);
    }, [download]);

    if (!download) return null;

    const displaySize = totalSize === 0 ? "Unknown" : formatBytes(totalSize as number);

    const handleDelete = async (e: React.MouseEvent) => {
        e.preventDefault();
        setIsDeleting(true);

        try {
            await fetch(`http://localhost:3211/delete-download?id=${id}&from_disk=${deleteFromDisk}`, {
                method: "DELETE",
            });
        } catch (error) {
            console.error("Failed to delete", error);
        }
        
        setIsDeleting(false);
    };

    return <>
        <TableRow>
            <TableCell className="font-medium">{download.name}</TableCell>
            <TableCell>{displaySize}</TableCell>
            <TableCell>{progress.toFixed(1)}%</TableCell>
            <TableCell className="text-right">{download.status}</TableCell>
            <TableCell className="text-right">
                <AlertDialog>
                    <AlertDialogTrigger asChild>
                        <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive hover:text-destructive/90 cursor-pointer">
                            {isDeleting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                        </Button>
                    </AlertDialogTrigger>
                    <AlertDialogContent>
                        <AlertDialogHeader>
                            <AlertDialogTitle>Remove Download?</AlertDialogTitle>
                            <AlertDialogDescription>
                                This will remove <b>{download.name}</b> from the list.
                            </AlertDialogDescription>
                        </AlertDialogHeader>
                        
                        {/* Checkbox for Disk Deletion */}
                        <div className="flex items-center space-x-2 py-4">
                            <Checkbox 
                                id={`delete-disk-${id}`} 
                                className="cursor-pointer"
                                checked={deleteFromDisk}
                                onCheckedChange={(checked) => setDeleteFromDisk(checked === true)}
                            />
                            <Label htmlFor={`delete-disk-${id}`} className="cursor-pointer">
                                Also delete files from disk
                            </Label>
                        </div>

                        <AlertDialogFooter>
                            <AlertDialogCancel onClick={() => setDeleteFromDisk(false)} className="cursor-pointer">
                                Cancel
                            </AlertDialogCancel>
                            <AlertDialogAction 
                                onClick={handleDelete}
                                className="bg-destructive hover:bg-destructive/90 text-destructive-foreground cursor-pointer"
                            >
                                Remove
                            </AlertDialogAction>
                        </AlertDialogFooter>
                    </AlertDialogContent>
                </AlertDialog>
            </TableCell>
        </TableRow>
    </>
});