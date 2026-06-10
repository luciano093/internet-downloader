import { useDownloadStore } from "@/stores/downloadStore";
import { useMemo } from "react";
import { getFilterCategory, type FilterCategory } from "../lib/filters";

type Counts = {
  all: number,
  status: Record<FilterCategory, number>,
  hosts: Record<string, number>,
}

export function useDownloadCounts() {
  const downloadIds = useDownloadStore((store) => store.downloadIds);
  const downloads = useDownloadStore((store) => store.downloads); 

  return useMemo(() => {
    // Defined here instead of inline in counts because 
    // TS complains {} has no index signature when using
    // the `satisfies` keyword
    const hosts: Record<string, number> = {};
    
    const counts = {
      all: downloadIds.length,
      status: {
        active: 0,
        paused: 0,
        completed: 0,
        failed: 0,
      } satisfies Record<FilterCategory, number>,
      hosts: hosts,
    } satisfies Counts;

    for (const id of downloadIds) {
      const download = downloads[id];
      if (!download) continue;

      // Status counts
      const category = getFilterCategory(download.status, download.is_paused);
      counts.status[category] = (counts.status[category]) + 1;

      // Hosts counts
      const seenHosts = new Set<string>();
      
      for (const file of Object.values(download.files)) {
        if (file.host && !seenHosts.has(file.host)) {
          seenHosts.add(file.host);
          counts.hosts[file.host] = (counts.hosts[file.host] || 0) + 1;
        }
      }
    }

    return counts;
  }, [downloadIds, downloads]);
}
