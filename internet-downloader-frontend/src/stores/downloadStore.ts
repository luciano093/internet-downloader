import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import type { DeltaEvent, DownloadItem, FileItem, FolderItem } from '../downloadTypes';
import type { FilterCategory } from '@/routes/downloads/lib/filters';

export type DownloadState = {
  downloads: Record<number, DownloadItem>;
  downloadIds: number[];
  selectedId: number | null;

  setSnapshot: (items: DownloadItem[]) => void;
  applyDelta: (delta: DeltaEvent) => void;
  setSelectedId: (id: number | null) => void;

  // Filters
  statusFilter: FilterCategory | null;
  setStatusFilter: (status: FilterCategory | null) => void;
  hostFilter: string | null;
  setHostFilter: (host: string | null) => void;
};

export const useDownloadStore = create<DownloadState>()(
    immer((set) => ({
        downloads: {},
        downloadIds: [],
        selectedId: null,

        setSnapshot: (items) => set((state) => {
          const newIds = new Set(items.map(i => i.id));
          
          // Remove stale downloads
          Object.keys(state.downloads).forEach(id => {
            if (!newIds.has(Number(id))) {
              delete state.downloads[Number(id)];
            }
          });
          
          state.downloadIds = items.map(i => i.id);
          
          items.forEach(item => {
            if (!item.files) item.files = {};
            if (!item.folders) item.folders = {};
            
            const existing = state.downloads[item.id];
            if (existing?.active_operation && !item.active_operation) {
              item.active_operation = existing.active_operation;
            }

            // Preserve is_paused from existing state if snapshot doesn't have it
            if (existing && item.is_paused === undefined) {
              item.is_paused = existing.is_paused;
            }

            // Don't let snapshot regress bytes_downloaded
            if (existing) {
              for (const [fileId, fileSnapshot] of Object.entries(item.files)) {
                const existingFile = existing.files?.[Number(fileId)];
                if (existingFile && existingFile.bytes_downloaded > (fileSnapshot.bytes_downloaded || 0)) {
                  fileSnapshot.bytes_downloaded = existingFile.bytes_downloaded;
                }
              }
            }
            
            state.downloads[item.id] = item;
          });
        }),

        applyDelta: (delta) => set((state) => {
            if (delta.action === "added") {
              if (!state.downloadIds.includes(delta.id)) {
                state.downloadIds.push(delta.id);
              }   
              
              state.downloads[delta.id] = delta.download as DownloadItem;
              
              return;
            }

            if (delta.action === "deleted") {
                delete state.downloads[delta.id];
                const index = state.downloadIds.indexOf(delta.id);
                state.downloadIds.splice(index, 1);
                return;
            }

            if (delta.action === "changes") {
                Object.entries(delta.changes).forEach(([idString, change]) => {
                    const id = Number(idString);
                    const download = state.downloads[change.id || id];

                    if (!download) return;

                    if (change.url) download.url = change.url;
                    if (change.status) download.status = change.status;
                    if (change.active_operation !== undefined) download.active_operation = change.active_operation;
                    if (change.is_paused !== undefined) download.is_paused = change.is_paused;

                  if (change.files) {
                    if (!download.files) {
                      download.files = {};
                    }
                    
                    Object.entries(change.files).forEach(([fileIdString, fileChanges]) => {
                        const fileId = Number(fileIdString);
                        const file = download.files[fileId];

                        if (file) {
                          Object.assign(file, fileChanges);
                        }

                        // If it's new (and the update contains the full object), add it
                        else if (fileChanges.file_name) {
                          download.files[fileId] = {
                            id: fileId,
                            ...fileChanges
                          } as FileItem;
                        }
                      });
                    }

                  if (change.folders) {
                    if (!download.folders) {
                      download.folders = {};
                    }
                    
                    Object.entries(change.folders).forEach(([folderIdString, folderChanges]) => {
                        const folderId = Number(folderIdString);
                        const folder = download.folders[folderId];

                        if (folder) {
                          Object.assign(folder, folderChanges);
                        } 

                        // If it's new (and the update contains the full object), add it
                        else if (folderChanges.folder_name !== undefined) {
                          download.folders[folderId] = {
                            id: folderId,
                            ...folderChanges
                          } as FolderItem;
                        }
                      });
                    }
                });
            }
        }),

        setSelectedId: (id) => set((state) => {
            state.selectedId = id;
        }),

        // Filters
        statusFilter: null,
        hostFilter: null,
        
        setStatusFilter: (status) => set((state) => {
          state.statusFilter = status;
        }),
        
        setHostFilter: (host) => set((state) => {
          state.hostFilter = host;
        }),
    }))
);
