import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import type { DeltaEvent, DownloadItem, DownloadNode } from './downloadTypes';

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
                    if (change.host) download.host = change.host;

                    if (change.files) {
                        Object.entries(change.files).forEach(([fileIdString, fileChanges]) => {
                            const fileId = Number(fileIdString);
                            const file = download.files[fileId];

                            if (file) {
                                Object.assign(file, fileChanges);
                            } 

                            // If it's new (and the update contains the full object), add it
                            else if (fileChanges.type) {
                                download.files[fileId] = fileChanges as DownloadNode;
                            }
                        })
                    }
                });
            }
        }),
    }))
);