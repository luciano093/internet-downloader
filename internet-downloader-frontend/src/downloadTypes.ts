export type FileItem = {
  type: "file";
  file_name: string;
  relative_path: string;
  status: string;
  url: string;
  hash: string;
  size: "unknown" | number;
  bytes_downloaded: number;
};

export type FolderItem = {
  type: "folder";
  folder_name: string;
  children: number[];
  status: string;
};

export type DownloadNode = FileItem | FolderItem;

export interface DownloadItem {
  url: string;
  status: string;
  host: string;
  name: string,
  files: Record<number, DownloadNode>;
}

export type FileItemDiff = {
  type: "file"; 
  file_name?: string;
  relative_path?: string;
  status?: string;
  url?: string;
  hash?: string | null;
  size?: "unknown" | number;
};

export type FolderItemDiff = {
  type: "folder";
  folder_name?: string;
  children?: number[];
  status?: string;
};

export type DownloadNodeDiff = FileItemDiff | FolderItemDiff;

export interface DownloadItemDiff {
  url?: string,
  status?: string,
  host?: string,
  relative_path?: string,
  files?: Record<number, DownloadNodeDiff>;
}

export type DeltaEvent = {
  id: number
  action: "added"
  download: DownloadItem
} | {
  id: number  
  action: "modified"
  changes: DownloadItemDiff
}