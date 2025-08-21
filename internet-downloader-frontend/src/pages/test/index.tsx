import { useEffect, useState } from "react";
import DownloadTask from "../../components/DownloadTask";
import DownloadUrlBar from "../../components/DownloadUrlBar";
import { DownloadCell, DownloadGrid, DownloadRow } from "../../components/DownloadGrid";

interface DownloadItem {
  url: string,
  status: string,
  host: string,
  folder?: {
    name: string,
    status: string,
    subfolders: []
    files: {
      name: string,
      url: string,
      status: string,
      progress: string,
      hash: string,
    }[]
  },
  file?: {
    name: string,
  }
}

type Download = DownloadItem & (
  | { folder: {
      name: string,
      status: string,
      subfolders: []
      files: {
        name: string,
        url: string,
        status: string,
        progress: string,
        hash: string,
      }[]
  }, }
  | { 
    file: { 
      name: string 
    }
   }
);

export default function Page() {
  const [downloads, setDownloads] = useState<Download[]>();

  useEffect(() => {
    const downloadsSource = new EventSource("http://localhost:3211/downloads");
    downloadsSource.onmessage = (event) => {
      const json = JSON.parse(event.data);

      setDownloads(json.downloads);
      console.log(json.downloads);
    }

    downloadsSource.onerror = (event) => console.log('Error:', event);

    return () => {
      downloadsSource.close();
    }
  }, [])

  return <>
    <DownloadUrlBar />
    <DownloadGrid>
      <DownloadRow>
        <DownloadCell title="Name" isHeader={true}></DownloadCell>
        <DownloadCell title="Size" isHeader={true}></DownloadCell>
        <DownloadCell title="Status" isHeader={true}></DownloadCell>
      </DownloadRow>

      {downloads && downloads.map((download) => {
        const name = (download.folder?.name ?? download.file?.name) as string;

        return <div key={download.url}>
          <DownloadRow>
            <DownloadCell>
              <DownloadTask text={name} />
            </DownloadCell>
            <DownloadCell>
              <DownloadTask text="100 GB" />
            </DownloadCell>
            <DownloadCell>
              <DownloadTask text={download.status} />
            </DownloadCell>
          </DownloadRow>
        </div>
      }
      )}
     </DownloadGrid>
  </>
}