import type { ActiveOperation, DownloadFailureReason, DownloadStatus, FileFailureReason, FileStatus } from "@/downloadTypes";

export function formatFileFailure(reason: FileFailureReason): string {
  switch (reason.state) {
    case "hash_mismatch": return "Hash Mismatch";
    case "disk_error": return "Disk Error";
    case "client_error": return "Client Error";
    case "server_error": return "Server Error";
    case "metadata_fetch_error": return "Metadata Fetch Error";
    case "bad_path": return "Bad Path";
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
    case "files_missing_from_disk": return "Files Missing from Disk";
    case "state_desynchronized": return "State Error";
    case "bad_path": return "Bad Path";
    default: return "Unknown Error";
  }
}

export function formatDownloadStatus(status: DownloadStatus): string {
  switch (status.state) {
    case "uninitialized": return "Queued";
    case "metadata_fetched": return "Ready";
    case "partial": return "Downloading...";
    case "completed": return "Completed";
    case "completed_with_errors": return "Completed (with errors)";
    case "not_found": return "Not Found";
    case "failed":
      return `Failed: ${formatDownloadFailure(status.value)}`;
  }
}

export function formatFileStatus(status: FileStatus): string {
  switch (status.state) {
    case "uninitialized": return "Queued";
    case "metadata_fetched": return "Ready";
    case "partial": return "Downloading...";
    case "completed": return "Completed";
    case "not_found": return "Not Found";
    case "failed":
        return `Failed: ${formatFileFailure(status.value)}`;
  }
}

export function formatActiveOperation(operation: ActiveOperation): string {
  switch (operation.state) {
    case "verifying": return "Verifying...";
    case "queued": return "Queued";
    case "downloading": return "Downloading...";
    case "waiting":
      return operation.value !== null ? `Waiting (${operation.value}s)` : "Waiting...";
  }
}
