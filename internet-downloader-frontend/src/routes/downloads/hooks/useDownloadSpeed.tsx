import { useEffect, useRef, useState } from "react";

export default function useDownloadSpeed(downloadedSize: number, status: string) {
    const [downloadSpeed, setDownloadSpeed] = useState(0);
    
    const currentSizeRef = useRef(downloadedSize);
    const statusRef = useRef(status);

    useEffect(() => {
        currentSizeRef.current = downloadedSize;
        statusRef.current = status;
    }, [downloadedSize, status]);

    const statsHistoryRef = useRef<{time: number, size: number}[]>([]); 

    useEffect(() => { 
        const TICK_RATE = 400; 
        const WINDOW_SIZE = 1000; 

        const interval = setInterval(() => {
            const now = performance.now();
            const currentSize = currentSizeRef.current;
            const currentStatus = statusRef.current?.toLowerCase();

            // Clear speed if stopped or paused
            if (currentStatus !== 'downloading') {
                setDownloadSpeed(0);
                statsHistoryRef.current =[];
                return;
            }

            statsHistoryRef.current.push({ time: now, size: currentSize });

            const threshold = now - WINDOW_SIZE;
            statsHistoryRef.current = statsHistoryRef.current.filter(s => s.time > threshold);

            if (statsHistoryRef.current.length > 1) {
                const first = statsHistoryRef.current[0];
                const last = statsHistoryRef.current[statsHistoryRef.current.length - 1];
                
                const bytesGained = last.size - first.size;
                const timePassed = (last.time - first.time) / 1000; 

                if (timePassed > 0 && bytesGained >= 0) {
                    const speed = bytesGained / timePassed;
                    setDownloadSpeed(prev => (0.3 * speed) + (0.7 * prev));
                }
            }
        }, TICK_RATE);

        return () => clearInterval(interval);
    }, [status]);

    return downloadSpeed;
}