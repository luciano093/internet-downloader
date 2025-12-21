import { useDownloadStore } from "@/store";
import { memo, useEffect, useMemo, useRef, useState } from "react";
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
    if (bytes === 0) return '0 B';
    if (bytes < 0 || isNaN(bytes)) return '0 B';

    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

    let i = Math.floor(Math.log(bytes) / Math.log(k));

    i = Math.max(0, Math.min(i, sizes.length - 1));
    
    if (i >= sizes.length) i = sizes.length - 1;

    const value = bytes / Math.pow(k, i);
    
    return `${value.toFixed(dm).replace(/\.00$/, '')} ${sizes[i]}`;
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
        const downloaded = file.bytes_downloaded || 0;

        totalBytes += size;
        downloadedBytes += downloaded;
    });

    const effectiveTotal = Math.max(totalBytes, downloadedBytes);

    const percentage = effectiveTotal === 0 ? 0 : (downloadedBytes / effectiveTotal) * 100;

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
    
    const { progress, totalSize, downloadedSize } = useMemo(() => {
        if (!download || !download.files || !download.files[0]) {
            return { progress: 0, totalSize: 0, downloadedSize: 0 };
        }
        
        return getFolderStats(download.files);
    }, [download]);

    const [downloadSpeed, setDownloadSpeed] = useState(0);
    const currentSizeRef = useRef(downloadedSize);

    useEffect(() => {
        currentSizeRef.current = downloadedSize;
    }, [downloadedSize]);

    const statsHistoryRef = useRef<{time: number, size: number}[]>([]); 
    
    useEffect(() => { 
        const TICK_RATE = 400; // in milliseconds
        const WINDOW_SIZE = 1000; // calculate speed over the last 1 second

        const interval = setInterval(() => {
        const now = performance.now();
        const currentSize = currentSizeRef.current || 0;

        // add current snapshot to history
        statsHistoryRef.current.push({ time: now, size: currentSize });

        // remove snapshots older than the 1-second window
        const threshold = now - WINDOW_SIZE;
        statsHistoryRef.current = statsHistoryRef.current.filter(s => s.time > threshold);

        // calculate speed based on the window
        if (statsHistoryRef.current.length > 1) {
            const first = statsHistoryRef.current[0];
                const last = statsHistoryRef.current[statsHistoryRef.current.length - 1];
                
                const bytesGained = last.size - first.size;
                const timePassed = (last.time - first.time) / 1000; // in seconds

                if (timePassed > 0) {
                    const speed = bytesGained / timePassed;
                    
                    // use smoothing 
                    setDownloadSpeed(prev => (0.3 * speed) + (0.7 * prev));
                }
            }

            // if download is stopped  clear the speed
            if (download.status === 'completed') setDownloadSpeed(0);

        }, TICK_RATE);

        return () => {
            clearInterval(interval);
            statsHistoryRef.current = [];
        };
    }, [download.status]);

    if (!download) return null;

    const displaySize = totalSize === 0 ? "Unknown" : formatBytes(totalSize as number);
    const isFinished = download.status === 'completed' || download.status === 'error';
    const displaySpeed = isFinished
        ? (download.status === 'completed' ? 'Done' : 'Failed')
        : `${formatBytes(downloadSpeed)}/s`;

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
            <TableCell>{displaySpeed}</TableCell>
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