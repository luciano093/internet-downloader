import { useDownloadStore } from "@/store";
import { memo, useMemo } from "react";
import { TableCell, TableRow } from "./ui/table";
import type { DownloadNode, FileItem } from "@/downloadTypes";

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

    return <>
        <TableRow>
            <TableCell className="font-medium">{download.name}</TableCell>
            <TableCell>{displaySize}</TableCell>
            <TableCell>{progress.toFixed(1)}%</TableCell>
            <TableCell className="text-right">{download.status}</TableCell>
        </TableRow>
    </>
});