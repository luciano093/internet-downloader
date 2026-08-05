import { getDownloadStats } from "@/lib/utils";
import { useDownloadStore } from "@/stores/downloadStore";
import { useEffect, useRef, useState } from "react";

type SpeedMap = Record<number, number>;

export function useDownloadSpeeds() {
  const downloads = useDownloadStore(store => store.downloads);
  const [speeds, setSpeeds] = useState<SpeedMap>({});
  const [aggregateSpeed, setAggregateSpeed] = useState(0);

  const downloadsRef = useRef(downloads);
  downloadsRef.current = downloads;
  const historyRef = useRef<Record<number, { time: number; size: number }[]>>({});

  const speedsRef = useRef<SpeedMap>({});
  speedsRef.current = speeds;

  useEffect(() => {
    const TICK_RATE = 400;
    const WINDOW_SIZE = 1000;

    const interval = setInterval(() => {
      const now = performance.now();
      const currentDownloads = downloadsRef.current;
      const newSpeeds: SpeedMap = {};
      let totalSpeed = 0;

      for (const download of Object.values(currentDownloads)) {
        const id = download.id;
        const downloadedSize = getDownloadStats(download).downloadedSize;
        const isDownloading = download.status?.state === "partial" && !download.is_paused;

        if (!isDownloading) {
          // not downloading, speed is 0, clear history so it resets
          historyRef.current[id] = [];
          newSpeeds[id] = 0;
          continue;
        }

        const history = historyRef.current[id] ?? [];
        history.push({ time: now, size: downloadedSize });

        const threshold = now - WINDOW_SIZE;
        const current = history.filter(sample => sample.time > threshold);
        historyRef.current[id] = current;

        if (current.length > 1) {
          const first = current[0];
          const last = current[current.length - 1];
          const bytesGained = last.size - first.size;
          const timePassed = (last.time - first.time) / 1000;

          if (timePassed > 0 && bytesGained >= 0) {
            const rawSpeed = bytesGained / timePassed;
            // smooth against the previous speed for this download
            const prevSpeed = speedsRef.current[id] ?? 0;
            const smoothed = 0.3 * rawSpeed + 0.7 * prevSpeed;
            newSpeeds[id] = smoothed;
            totalSpeed += smoothed;
          } else {
            newSpeeds[id] = speedsRef.current[id] ?? 0;
            totalSpeed += newSpeeds[id];
          }
        } else {
          newSpeeds[id] = speedsRef.current[id] ?? 0;
          totalSpeed += newSpeeds[id];
        }
      }

      speedsRef.current = newSpeeds;
      setSpeeds(newSpeeds);
      setAggregateSpeed(totalSpeed);
    }, TICK_RATE);

    return () => clearInterval(interval);
  }, []);

  return { speeds, aggregateSpeed };
}
