import { useDownloadStore } from "@/stores/downloadStore";
import { useMemo } from "react";
import { STATE_TO_CATEGORY, type FilterCategory } from "../components/DownloadsSidebar";

type counts = {
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
    } as counts;

    for (const id of downloadIds) {
      const download = downloads[id];
      if (!download) continue;

      const category = STATE_TO_CATEGORY[download.status.state];
      counts.status[category] = (counts.status[category] || 0) + 1;
    }

    return counts;
  }, [downloadIds, downloads]);
}
