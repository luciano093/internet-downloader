
struct Download {
    download_type: DownloadType,
}

enum DownloadType {
    Folder (FolderDownload),
    File (FileDownload),
}

struct FileDownload {
    status: DownloadStatus,
}

struct FolderDownload {
    files: Vec<FileDownload>,
}

enum DownloadStatus {
    Queued,
    InProgress,
    Completed,
    Paused,
    Failed,
}