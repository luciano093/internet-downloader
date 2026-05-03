export type FileFailureReason = 
  | { state: "network_error" }
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

export type ActiveOperation = "verifying";

export type FileItem = {
  id: number;
  parent_id: number | null;
  file_name: string;
  relative_path: string;
  status: FileStatus;
  active_operation: ActiveOperation | null,
  url: string;
  hash: string | null;
  size: "unknown" | number;
  bytes_downloaded: number;
};

export type FolderItem = {
  id: number;
  parent_id: number | null;
  folder_name: string;
  relative_path: string;
  child_files: number[];
  child_folders: number[];
  status: DownloadStatus;
  active_operation: ActiveOperation | null,
};

export type DownloadNode = FileItem | FolderItem;

export interface DownloadItem {
  id: number,
  name: string,
  url: string;
  host: string;
  status: DownloadStatus;
  active_operation: ActiveOperation | null,
  
  files: Record<number, FileItem>;
  folders: Record<number, FolderItem>;
}

export type FileItemDiff = { 
  file_name?: string;
  relative_path?: string;
  status?: FileStatus;
  active_operation?: ActiveOperation | null,
  url?: string;
  hash?: string | null;
  size?: "unknown" | number;
  bytes_downloaded?: number;
};

export type FolderItemDiff = {
  folder_name?: string;
  status?: DownloadStatus;
  active_operation?: ActiveOperation | null,
  child_files?: number[];
  child_folders?: number[];
};

export type DownloadNodeDiff = FileItemDiff | FolderItemDiff;

export interface DownloadItemDiff {
  id: number,
  url?: string,
  status?: DownloadStatus,
  active_operation?: ActiveOperation | null,
  host?: string,
  relative_path?: string,
  files: Record<number, FileItemDiff>;
  folders: Record<number, FolderItemDiff>;
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
