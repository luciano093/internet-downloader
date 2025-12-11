import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import type { DeltaEvent, DownloadItem, DownloadItemDiff, DownloadNode } from './downloadTypes';

type DownloadState = {
    downloads: Record<number, DownloadItem>;
    downloadIds: number[];

    setSnapshot: (items: DownloadItem[]) => void;
    applyDelta: (delta: DeltaEvent) => void;
};

export const useDownloadStore = create<DownloadState>()(
    immer((set) => ({
        downloads: {},
        downloadIds: [],

        setSnapshot: (items) => set((state) => {
            state.downloadIds = items.map(i => i.id);
            state.downloads = {};
            items.forEach(item => {
                if (!item.files) {
                    item.files = {};
                }

                state.downloads[item.id] = item;
            })
        }),

        applyDelta: (delta) => set((state) => {
            if (delta.action === "added") {
                state.downloads[delta.id] = delta.download as DownloadItem;
                state.downloadIds.push(delta.id);
                return;
            }

            if (delta.action === "modified") {
                const item = state.downloads[delta.id];
                if (!item) return;

                const diff = delta.changes as DownloadItemDiff;
                
                if (diff.url !== undefined) item.url = diff.url;
                if (diff.status !== undefined) item.status = diff.status;
                if (diff.host !== undefined) item.host = diff.host;

                if (diff.files) {
                    Object.entries(diff.files).forEach(([fileId, nodeUpdate]) => {
                        if (item.files[fileId]) {
                            Object.assign(item.files[fileId], nodeUpdate);
                        } 
                        // If it's new (and the update contains the full object), add it
                        else if (nodeUpdate.type) {
                            item.files[fileId] = nodeUpdate as DownloadNode;
                        }
                    });
                }
            }
        }),
    }))
);