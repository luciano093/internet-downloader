import type { DownloadItem } from "@/downloadTypes";
import { useVirtualizer, type VirtualItem } from "@tanstack/react-virtual";
import { File, Folder, FolderOpen, type LucideIcon } from "lucide-react";
import { useMemo, useRef, useState, type ReactNode } from "react"

type VirtualDownloadItem = {
  type: "folder" | "file";
  depth: number;
  id: number;
}

function FileTreeRow({ virtualRow, depth, icon: Icon, onClick, children }: { virtualRow: VirtualItem, depth: number, icon: LucideIcon, onClick?: React.MouseEventHandler<HTMLDivElement>, children?: ReactNode }) {
  return <div
    key={virtualRow.key}
    className="flex items-center gap-2 hover:bg-accent cursor-pointer text-[13px]"
    style={{
      position: 'absolute',
      top: 0,
      left: 0,
      width: '100%',
      height: `${virtualRow.size}px`,
      transform: `translateY(${virtualRow.start}px)`,
      paddingLeft: `${(depth * 16) + 8}px`
    }}
    onClick={onClick}
  >
    <Icon className="w-4 h-4 ml-1 opacity-70" />
    {children}
  </div>
}

export default function FileTree({ download }: { download: DownloadItem }) {
  const [expandedFolders, setExpandedFolders] = useState(new Set<number>());

  const virtualFileList = useMemo(() => {
    const virtualFileList: VirtualDownloadItem[] = [];

    const rootFolder = Object.values(download.folders).filter(folder => folder.parent_id == null)[0];
    const rootFile = Object.values(download.files).filter(file => file.parent_id == null)[0];

    console.log(rootFolder);
    console.log(rootFile);
    
    const stack: VirtualDownloadItem[] = [];
    
    if (rootFolder) {
      stack.push({
        type: "folder",
        depth: 0,
        id: rootFolder.id,
      } as VirtualDownloadItem);
    }

    if (rootFile) {
      stack.push({
        type: "file",
        depth: 0,
        id: rootFile.id,
      } as VirtualDownloadItem);
    }

    while (stack.length > 0) {
      const currentItem = stack.pop()!;

      virtualFileList.push(currentItem);

      if (currentItem.type === "folder" && expandedFolders.has(currentItem.id)) {
        const folder = download.folders[currentItem.id];
        if (!folder) continue;

        for (let i = folder.child_files.length - 1; i >= 0; i--) {
          stack.push({ 
            type: "file", 
            id: folder.child_files[i], 
            depth: currentItem.depth + 1 
          });
        }
    
        for (let i = folder.child_folders.length - 1; i >= 0; i--) {
          stack.push({ 
            type: "folder", 
            id: folder.child_folders[i], 
            depth: currentItem.depth + 1 
          });
        }
      }
    }
  
    return virtualFileList;
  }, [expandedFolders, download]);

  const parentRef = useRef<HTMLDivElement>(null);

  const rowVirtualizer = useVirtualizer({
    count: virtualFileList.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 23.5,
    overscan: 10,
  });
  
  return <>
    <div className="py-2 min-w-max relative flex-1 h-full">
      <div ref={parentRef} className="h-full w-full overflow-auto">
        <div 
          style={{ 
            height: `${rowVirtualizer.getTotalSize()}px`, 
            width: '100%', 
            position: 'relative' 
          }}
        >
          {rowVirtualizer.getVirtualItems().map((virtualRow) => {
            console.log(virtualRow);
            const item = virtualFileList[virtualRow.index];
            console.log(item);
            

            if (item.type === "folder") {
              const folder = download.folders[item.id];

              const isExpanded = expandedFolders.has(folder.id);

              return <>
                <FileTreeRow
                  virtualRow={virtualRow}
                  depth={item.depth}
                  icon={isExpanded ? FolderOpen : Folder}
                  onClick={() => {
                    setExpandedFolders((prev) => {
                      const next = new Set(prev);
                      if (next.has(folder.id)) next.delete(folder.id);
                      else next.add(folder.id);
                      return next;
                    });
                  }}>
                  {folder.folder_name}
                </FileTreeRow>
              </>
            } else {
              const file = download.files[item.id];

              return <>
                <FileTreeRow virtualRow={virtualRow} depth={item.depth} icon={File}>
                  {file.file_name}
                </FileTreeRow>
              </>
            }
          })}
        </div>
      </div>
  </div>
  </>
}
