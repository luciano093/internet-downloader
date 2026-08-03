import type { DownloadItem, FileItem } from "@/downloadTypes";
import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function getFolderStats(files: Record<number, FileItem>) {
    const allFiles = Object.values(files);

    if (allFiles.length === 0) {
        return { progress: 0, totalSize: 0, downloadedSize: 0 };
    }

    let totalBytes = 0;
    let downloadedBytes = 0;

    allFiles.forEach(file => {
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

export function getDownloadStats(download: DownloadItem) {
  return getFolderStats(download.files);
}
