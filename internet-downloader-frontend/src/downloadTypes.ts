export type FileFailureReason = 
  | { state: "hash_mismatch" }
  | { state: "disk_error" }
  | { state: "client_error" }
  | { state: "server_error" }
  | { state: "metadata_fetch_error" }
  | { state: "bad_path" }
  | { state: "unknown" };

export type DownloadFailureReason =
  | { state: "hash_mismatch" }
  | { state: "disk_error" }
  | { state: "client_error" }
  | { state: "server_error" }
  | { state: "metadata_fetch_error" }
  | { state: "multiple_errors" }
  | { state: "all_files_failed"; value: FileFailureReason }
  | { state: "files_missing_from_disk" }
  | { state: "state_desynchronized" }
  | { state: "bad_path" }
  | { state: "unknown" };

export type FileStatus =
  | { state: "uninitialized" }
  | { state: "metadata_fetched" }
  | { state: "partial" }
  | { state: "completed" }
  | { state: "not_found" }
  | { state: "failed"; value: FileFailureReason };

export type DownloadStatus =
  | { state: "uninitialized" }
  | { state: "metadata_fetched" }
  | { state: "partial" }
  | { state: "completed" }
  | { state: "completed_with_errors" }
  | { state: "not_found" }
  | { state: "failed"; value: DownloadFailureReason };

export type ActiveOperation = 
  | { state: "verifying" }
  | { state: "queued" }
  | { state: "downloading" }
  | { state: "waiting"; value: number | null };

export type FileItem = {
  id: number;
  parent_id: number | null;
  file_name: string;
  relative_path: string;
  status: FileStatus;
  active_operation: ActiveOperation | null,
  is_paused: boolean,
  url: string;
  host: string;
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
  is_paused: boolean,
};

export type DownloadNode = FileItem | FolderItem;

export interface DownloadItem {
  id: number,
  name: string,
  url: string;
  status: DownloadStatus;
  active_operation: ActiveOperation | null,
  is_paused: boolean,
  
  files: Record<number, FileItem>;
  folders: Record<number, FolderItem>;
}

export type FileItemDiff = { 
  parent_id?: number | null; 
  file_name?: string;
  relative_path?: string;
  status?: FileStatus;
  active_operation?: ActiveOperation | null,
  is_paused?: boolean,
  url?: string;
  host?: string;
  hash?: string | null;
  size?: "unknown" | number;
  bytes_downloaded?: number;
};

export type FolderItemDiff = {
  parent_id?: number | null; 
  folder_name?: string;
  status?: DownloadStatus;
  active_operation?: ActiveOperation | null,
  is_paused?: boolean,
  child_files?: number[];
  child_folders?: number[];
};

export type DownloadNodeDiff = FileItemDiff | FolderItemDiff;

export interface DownloadItemDiff {
  id: number,
  url?: string,
  status?: DownloadStatus,
  active_operation?: ActiveOperation | null,
  is_paused?: boolean,
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

export type AppSettings = {
  global_speed_limit: number | null;
  default_save_path: string | null;
  download_settings: Record<number, { speed_limit: number | null }>;
  host_settings: Record<string, { speed_limit: number | null }>;
};
