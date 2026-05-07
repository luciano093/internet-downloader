import type { DownloadFailureReason, DownloadStatus, FileFailureReason, FileStatus } from "@/downloadTypes";

export function formatFileFailure(reason: FileFailureReason): string {
    switch (reason.state) {
        case "hash_mismatch": return "Hash Mismatch";
        case "disk_error": return "Disk Error";
        case "network_error": return "Network Error";
        default: return "Unknown Error";
    }
}

export function formatDownloadFailure(reason: DownloadFailureReason): string {
    switch (reason.state) {
        case "disk_error": return "Disk Error";
        case "hash_mismatch": return "Hash Mismatch";
        case "client_error": return "Client Error";
        case "server_error": return "Server Error";
        case "metadata_fetch_error": return "Metadata Fetch Error";
        case "multiple_errors": return "Multiple Errors";
        case "all_files_failed": 
        return `All Files Failed (${formatFileFailure(reason.value)})`; 
        default: return "Unknown Error";
    }
}

export function formatDownloadStatus(status: DownloadStatus): string {
    switch (status.state) {
        case "queued": return "Queued";
        case "initializing": return "Initializing...";
        case "fetching_metadata": return "Fetching Metadata...";
        case "in_progress": return "Downloading";
        case "completed": return "Completed";
        case "completed_with_errors": return "Completed (with errors)";
        case "paused": return "Paused";
        case "not_found": return "Missing from Disk";
        case "retrying": return "Retrying...";
        case "waiting":
        return status.value !== null ? `Waiting (${status.value}s)` : "Waiting...";
        case "failed":
        return `Failed: ${formatDownloadFailure(status.value)}`;
    }
}

export function formatFileStatus(status: FileStatus): string {
    switch (status.state) {
        case "queued": return "Queued";
        case "initializing": return "Initializing...";
        case "fetching_metadata": return "Fetching Metadata...";
        case "in_progress": return "Downloading";
        case "completed": return "Completed";
        case "paused": return "Paused";
        case "not_found": return "Not Found";
        case "retrying": return "Retrying...";
        case "waiting": 
        return status.value !== null ? `Waiting (${status.value}s)` : "Waiting...";
        case "failed":
        return `Failed: ${formatFileFailure(status.value)}`;
    }
}
