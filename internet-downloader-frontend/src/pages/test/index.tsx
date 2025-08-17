import React, { type ReactNode } from "react";
import DownloadTask from "../../components/DownloadTask";
import DownloadUrlBar from "../../components/DownloadUrlBar";

function DownloadColumn({ title, children }: { title: string, children: ReactNode}) {
  return <>
    <div>
      <div className="px-2 py-1 border-r border-b border-gray-800 bg-gray-700">
        {title}
      </div>
      <div className="border-r border-gray-800">
        {children}
      </div>
    </div>
  </>
}

function DownloadGrid({ children }: { children: ReactNode }) {
  const columnCount = React.Children.count(children);
  
  return <>
    <div className="bg-gray-800 border border-gray-800">
      <div 
        className="grid"
        style={{ gridTemplateColumns: `repeat(${columnCount}, 1fr)` }}
      >
        {children}
      </div>
    </div>
  </>
}

export default function Page() {
  return <>
    <DownloadUrlBar />
    <DownloadGrid>
      <DownloadColumn title="Name">
        <DownloadTask text="[SubsPlease] Gachiakuta - 03 (1080p) [BF76DD32].mkv" />
        <DownloadTask text="[SubsPlease] Gachiakuta - 03 (1080p) [BF76DD32].mkv" />
      </DownloadColumn>
      <DownloadColumn title="Size">
        <DownloadTask text="100 GB" />
        <DownloadTask text="50 GB" />
      </DownloadColumn>
      <DownloadColumn title="Status">
        <DownloadTask text="Complete" />
        <DownloadTask text="Queued" />
      </DownloadColumn>
    </DownloadGrid>
  </>
}