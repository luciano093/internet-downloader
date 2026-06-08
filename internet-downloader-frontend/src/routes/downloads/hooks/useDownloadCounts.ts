import { useDownloadStore } from "@/stores/downloadStore";
import { useMemo } from "react";
import { getFilterCategory, type FilterCategory } from "../lib/filters";

type Counts = {
  all: number,
  status: Record<FilterCategory, number>,
}

export function useDownloadCounts() {
  const downloadIds = useDownloadStore((store) => store.downloadIds);
  const downloads = useDownloadStore((store) => store.downloads); 

  return useMemo(() => {
    const counts = {
      all: downloadIds.length,
      status: {
        active: 0,
        paused: 0,
        completed: 0,
        failed: 0,
      } satisfies Record<FilterCategory, number>,
    } satisfies Counts;

    for (const id of downloadIds) {
      const download = downloads[id];
      if (!download) continue;

      const category = getFilterCategory(download.status, download.is_paused);
      counts.status[category] = (counts.status[category]) + 1;
    }

    return counts;
  }, [downloadIds, downloads]);
}
