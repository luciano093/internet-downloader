import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

import { useVirtualizer } from "@tanstack/react-virtual";
import { useCallback, useEffect, useRef } from "react";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { useDownloadStore } from "@/store";
import { DownloadRow } from "@/components/DownloadRow";
import type { DeltaEvent } from "@/downloadTypes";

export default function Page() {
  const setSnapshot = useDownloadStore((s) => s.setSnapshot);
  const applyDelta = useDownloadStore((s) => s.applyDelta);
  const downloadIds = useDownloadStore((s) => s.downloadIds);

  const eventSourceRef = useRef<EventSource | null>(null);
  const tableContainerRef = useRef<HTMLDivElement>(null);

  const rowVirtualizer = useVirtualizer({
    count: downloadIds.length,
    getScrollElement: () => tableContainerRef.current,
    estimateSize: () => 53,
    overscan: 10,
  });

  const createEventSource = useCallback(() => {
    // Close existing connection if any
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }

    const newEventSource = new EventSource("http://localhost:3211/downloads");

    newEventSource.addEventListener("snapshot", (event) => {
      const snapshot = JSON.parse(event.data);
        
      setSnapshot(snapshot);
    });

    newEventSource.addEventListener("delta", (event) => {
      const delta = JSON.parse(event.data) as DeltaEvent;
      console.log("delta: ", delta)

      applyDelta(delta);
    });

    newEventSource.onerror = (event) => {
      console.log('Error:', event);
      if (newEventSource.readyState === EventSource.CLOSED) {
        console.log('Connection closed, attempting manual reconnect');
        setTimeout(() => {
          createEventSource();
        }, 2000);
      }
    };

    newEventSource.onopen = () => {
      console.log('SSE connection opened');
    };

    eventSourceRef.current = newEventSource;
  }, [applyDelta, setSnapshot]);

  useEffect(() => {
    createEventSource();

    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
      }
    }
  }, [createEventSource]);

  const onSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const formData = new FormData(event.currentTarget);

    const downloads = formData.get("downloadsTextArea");

    event.currentTarget.reset(); 

    fetch(`http://localhost:3211/add-download?url=${downloads}`, {
      method: "POST",
    })
  };

  const items = rowVirtualizer.getVirtualItems();
  const paddingTop = items.length > 0 ? items[0].start : 0;
  const paddingBottom = items.length > 0
    ? rowVirtualizer.getTotalSize() - items[items.length - 1].end
    : 0;

  return <>
    <form onSubmit={onSubmit}>
      <Textarea placeholder="Enter downloads here." name="downloadsTextArea" />
      <Button className="cursor-pointer" type="submit">Download</Button>
    </form>
    
    <div 
        ref={tableContainerRef} 
        className="rounded-md border h-[600px] overflow-auto relative"
      >
      <Table>
        <TableCaption>Downloads.</TableCaption>
        <TableHeader>
            <TableRow>
            <TableHead className="w-[100px]">Name</TableHead>
            <TableHead>Size</TableHead>
            <TableHead>Progress</TableHead>
            <TableHead className="text-right">Status</TableHead>
            <TableHead className="w-[50px]"></TableHead>
            </TableRow>
        </TableHeader>

        <TableBody>
          {paddingTop > 0 && (
            <TableRow>
              <TableCell style={{ height: `${paddingTop}px` }} colSpan={4} />
            </TableRow>
          )}

          {items.map((virtualRow: { index: number; }) => {
              const id = downloadIds[virtualRow.index];
              return <DownloadRow key={id} id={id} />;
          })}

          {paddingBottom > 0 && (
            <TableRow>
              <TableCell style={{ height: `${paddingBottom}px` }} colSpan={4} />
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  </>
}