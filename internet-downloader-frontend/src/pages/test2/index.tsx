import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

import { useCallback, useEffect, useRef, useState } from "react";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";

interface DownloadItem {
  id: number,
  url: string,
  status: string,
  host: string,
  folder?: {
    name: string,
    progress: string,
    status: string,
    subfolders: []
    files: {
      name: string,
      progress: string,
      url: string,
      status: string,
      hash: string,
    }[]
  },
  file?: {
    name: string,
    progress: string,
    url: string,
    status: string,
    hash: string,
  }
}

interface DownloadItemDiff {
  url?: string,
  status?: string,
  host?: string,
  folder?: {
    name?: string,
    progress?: string,
    status?: string,
    subfolders?: []
    files?: {
      name?: string,
      progress?: string,
      url?: string,
      status?: string,
      hash?: string,
    }[]
  },
  file?: {
    name?: string,
    progress?: string,
    url?: string,
    status?: string,
    hash?: string,
  }
}

type DeltaEvent = {
  id: number
  action: "created"
  changes: DownloadItem
} | {
  id: number  
  action: "modified"
  changes: DownloadItemDiff
}

export default function Page() {
  const [downloads, setDownloads] = useState<DownloadItem[]>();
  const eventSourceRef = useRef<EventSource | null>(null);

  const applyDiff = (item: DownloadItem, diff: DownloadItemDiff) => {
    if (diff.url !== undefined) item.url = diff.url
    if (diff.status !== undefined) item.status = diff.status
    if (diff.host !== undefined) item.host = diff.host
    
    if (diff.folder && item.folder) {
      if (diff.folder.name !== undefined) item.folder.name = diff.folder.name
      if (diff.folder.progress !== undefined) item.folder.progress = diff.folder.progress
      if (diff.folder.status !== undefined) item.folder.status = diff.folder.status
      
      // Files array updates
      if (diff.folder.files) {
        diff.folder.files.forEach((fileDiff, i) => {
          if (fileDiff && item.folder!.files[i]) {
            Object.assign(item.folder!.files[i], fileDiff)
          }
        })
      }
    }
    
    if (diff.file && item.file) {
      Object.assign(item.file, diff.file)
    }
  };

  const applyDeltas = useCallback((deltas: DeltaEvent[]) => {
    if (deltas.length > 0) {
      console.log("delta: ", deltas);
    }

    setDownloads(previousDownloads => {
      const newDownloads = [...previousDownloads ?? []];

      deltas.forEach(delta => {
        if (delta.action === "modified") {
          const item = newDownloads.find(item => item.id === delta.id)

          if (!item) {
            console.log("Item not found for id: ", delta.id);
            return;
          }

          applyDiff(item, delta.changes);
        }
      });

      return newDownloads;
    });
  }, []);

  const createEventSource = useCallback(() => {
    // Close existing connection if any
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }

    const newEventSource = new EventSource("http://localhost:3211/downloads");

    newEventSource.addEventListener("snapshot", (event) => {
      const snapshot = JSON.parse(event.data);
      console.log("snapshot: ", snapshot);
        
      setDownloads(snapshot.downloads);
    });

    newEventSource.addEventListener("delta", (event) => {
      console.log("here");
      const delta = JSON.parse(event.data);
      applyDeltas(delta.deltas);
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
  }, [applyDeltas, eventSourceRef]);

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
    console.log("test: ", downloads);

    event.currentTarget.reset(); 

    fetch(`http://localhost:3211/add-download?url=${downloads}`, {
      method: "POST",
    })
  };

  return <>
    <form onSubmit={onSubmit}>
      <Textarea placeholder="Enter downloads here." name="downloadsTextArea" />
      <Button className="cursor-pointer" type="submit">Download</Button>
    </form>
    
    <Table>
      <TableCaption>Downloads.</TableCaption>
      <TableHeader>
          <TableRow>
          <TableHead className="w-[100px]">Name</TableHead>
          <TableHead>Size</TableHead>
          <TableHead>Progress</TableHead>
          <TableHead className="text-right">Status</TableHead>
          </TableRow>
      </TableHeader>
      {downloads && downloads.map((download) => {
          const name = (download.folder?.name ?? download.file?.name) as string;
          const progress = (download.folder?.progress ?? download.file?.progress) as string;

          return <TableBody key={download.url}>
              <TableRow>
              <TableCell className="font-medium">{name}</TableCell>
              <TableCell>10.0GB</TableCell>
              <TableCell>{progress}</TableCell>
              <TableCell className="text-right">{download.status}</TableCell>
              </TableRow>
          </TableBody>
      })}
    </Table>
  </>
}