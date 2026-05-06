import { createFileRoute } from '@tanstack/react-router'
import AppLayout from '../components/AppLayout'
import { DownloadsTable } from './downloads/components/DownloadsTable'
import { useDownloadStore } from '@/stores/downloadStore'
import { useCallback, useEffect, useRef } from 'react'
import DownloadsSidebar, { STATE_TO_CATEGORY } from './downloads/components/DownloadsSidebar'
import DownloadsTopBar from './downloads/components/DownloadsTopBar'
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '@/components/ui/resizable'
import BottomDetailsPane from './downloads/components/BottomDetailsPane'

export const Route = createFileRoute('/')({
  component: Index,
})

function Index() {
  const setSnapshot = useDownloadStore((store) => store.setSnapshot);
  const applyDelta = useDownloadStore((store) => store.applyDelta);
  const downloadIds = useDownloadStore((store) => store.downloadIds);
  const downloadStore = useDownloadStore();
  const { selectedId, statusFilter } = useDownloadStore();

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
        console.log("snapshot:", JSON.parse(event.data));
        setSnapshot(JSON.parse(event.data));
      });

      newEventSource.addEventListener("delta", (event) => {
        console.log("delta:", JSON.parse(event.data));
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

  // Apply filters

  const filteredIds = downloadIds.filter(id => {
    const download = downloadStore.downloads[id];

    const downloadCategory = STATE_TO_CATEGORY[download.status.state];

    // We either get all downloads that match our current status filter
    // or otherwise, if the statusFilter is not set, we set this to true
    const matchesStatus = statusFilter == downloadCategory || statusFilter == null;

    return matchesStatus;
  });

    return <>
      <AppLayout
        topBar={<DownloadsTopBar />} 
        sidebarTop={<DownloadsSidebar />}
      >
        <ResizablePanelGroup orientation='vertical'>
          <ResizablePanel>
            <DownloadsTable downloadIds={filteredIds} />
          </ResizablePanel>
          { selectedId &&
            <>
              <ResizableHandle className="bg-border" />

              <ResizablePanel>
                <BottomDetailsPane />
              </ResizablePanel>
            </>
          }
       </ResizablePanelGroup>
      </AppLayout>
    </>
}
