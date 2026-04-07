import { createFileRoute } from '@tanstack/react-router'
import AppLayout from '../components/AppLayout'
import { DownloadsTable } from './downloads/components/DownloadsTable'
import { useDownloadStore } from '@/store'
import { useCallback, useEffect, useRef } from 'react'
import DownloadsSidebar from './downloads/components/DownloadsSidebar'
import DownloadsTopBar from './downloads/components/DownloadsTopBar'

export const Route = createFileRoute('/')({
  component: Index,
})

function Index() {
  const setSnapshot = useDownloadStore((store) => store.setSnapshot);
  const applyDelta = useDownloadStore((store) => store.applyDelta);
  const downloadIds = useDownloadStore((store) => store.downloadIds);

  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);

  const createEventSource = useCallback(() => {
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }

      const newEventSource = new EventSource("http://localhost:3211/downloads");

      newEventSource.addEventListener("snapshot", (event) => {
        setSnapshot(JSON.parse(event.data));
      });

      newEventSource.addEventListener("delta", (event) => {
        applyDelta(JSON.parse(event.data));
      });

      newEventSource.onerror = (event) => {
        console.log('Error:', event);
        newEventSource.close();
        reconnectTimeoutRef.current = setTimeout(() => createEventSource(), 500);
      };

      eventSourceRef.current = newEventSource;
    }, [applyDelta, setSnapshot]);

    useEffect(() => {
      createEventSource();
      return () => {
        if (eventSourceRef.current) eventSourceRef.current.close();
      }
    }, [createEventSource]);

    return <>
      <AppLayout
        topBar={<DownloadsTopBar />} 
        sidebarTop={<DownloadsSidebar />}
      >
       <DownloadsTable downloadIds={downloadIds} />
      </AppLayout>
    </>
}