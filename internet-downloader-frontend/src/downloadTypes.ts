export type FileFailureReason = 
  | { state: "network_error" } // Replace with your actual FileFailureReason variants
  | { state: "disk_error" }
  | { state: "hash_mismatch" }; 

export type DownloadFailureReason =
  | { state: "hash_mismatch" }
  | { state: "disk_error" }
  | { state: "client_error" }
  | { state: "server_error" }
  | { state: "metadata_fetch_error" }
  | { state: "multiple_errors" }
  | { state: "all_files_failed"; value: FileFailureReason };

export type FileStatus =
  | { state: "queued" }
  | { state: "initializing" }
  | { state: "fetching_metadata" }
  | { state: "in_progress" }
  | { state: "completed" }
  | { state: "paused" }
  | { state: "not_found" }
  | { state: "retrying" }
  | { state: "waiting"; value: number | null }
  | { state: "failed"; value: FileFailureReason };

export type DownloadStatus =
  | { state: "queued" }
  | { state: "initializing" }
  | { state: "fetching_metadata" }
  | { state: "in_progress" }
  | { state: "completed" }
  | { state: "completed_with_errors" }
  | { state: "paused" }
  | { state: "not_found" }
  | { state: "retrying" }
  | { state: "waiting"; value: number | null }
  | { state: "failed"; value: DownloadFailureReason };

export type ActiveOperation =
  | { active_operation: "verifying" };

export type FileItem = {
  type: "file";
  file_name: string;
  relative_path: string;
  status: FileStatus;
  active_operation: ActiveOperation | null,
  url: string;
  hash: string;
  size: "unknown" | number;
  bytes_downloaded: number;
};

export type FolderItem = {
  type: "folder";
  folder_name: string;
  children: number[];
  status: DownloadStatus;
  active_operation: ActiveOperation | null,
};

export type DownloadNode = FileItem | FolderItem;

export interface DownloadItem {
  url: string;
  status: DownloadStatus;
  active_operation: ActiveOperation | null,
  host: string;
  name: string,
  files: Record<number, DownloadNode>;
  id: number,
}

export type FileItemDiff = {
  type: "file"; 
  file_name?: string;
  relative_path?: string;
  status?: FileStatus;
  active_operation?: ActiveOperation,
  url?: string;
  hash?: string | null;
  size?: "unknown" | number;
};

export type FolderItemDiff = {
  type: "folder";
  folder_name?: string;
  children?: number[];
  status?: DownloadStatus;
  active_operation?: ActiveOperation,
};

export type DownloadNodeDiff = FileItemDiff | FolderItemDiff;

export interface DownloadItemDiff {
  url?: string,
  status?: DownloadStatus,
  active_operation?: ActiveOperation,
  host?: string,
  relative_path?: string,
  files?: Record<number, DownloadNodeDiff>;
  id: number,
}

export type DeltaEvent = {
  id: number
  action: "added"
  download: DownloadItem
} | {
  id: number
  action: "deleted"
} | {
  action: "changes"
  changes: Record<number, DownloadItemDiff>
}