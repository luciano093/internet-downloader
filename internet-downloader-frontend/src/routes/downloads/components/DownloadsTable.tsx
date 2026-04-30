

import { useReactTable, getCoreRowModel, flexRender, createColumnHelper } from "@tanstack/react-table";
import { TableHeader, TableRow, TableHead, TableBody, TableCell, Table } from "@/components/ui/table";
import { useEffect, useMemo, useRef, useState } from "react";
import { cn } from "@/lib/utils";
import { useDownloadStore } from "@/stores/downloadStore";
import type { DownloadItem, DownloadNode, FileItem } from "@/downloadTypes";
import { useVirtualizer } from "@tanstack/react-virtual";
import useDownloadSpeed from "../hooks/useDownloadSpeed";
import { formatDownloadStatus } from "@/lib/status_utils";

const SpeedCellContent = ({ download }: { download: DownloadItem }) => {
    const stats = getFolderStats(download.files);
    const speed = useDownloadSpeed(stats.downloadedSize, "downloading");

    useEffect(() => {
        console.log(`SpeedCell mounted for ${download.name}`);
        return () => console.log(`SpeedCell unmounted for ${download.name}`);
    },[]);

    return <span>{formatBytes(speed)}/s</span>;
};

function formatBytes(bytes: number, decimals = 2) {
    if (bytes === 0) return '0 B';
    if (bytes < 0 || isNaN(bytes)) return '0 B';

    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

    let i = Math.floor(Math.log(bytes) / Math.log(k));

    i = Math.max(0, Math.min(i, sizes.length - 1));
    
    if (i >= sizes.length) i = sizes.length - 1;

    const value = bytes / Math.pow(k, i);
    
    return `${value.toFixed(dm).replace(/\.00$/, '')} ${sizes[i]}`;
}

    
function getFolderStats(files: Record<number, DownloadNode>) {
    const allItems = Object.values(files);
    const activeFiles = allItems.filter((item): item is FileItem => item.type === 'file');

    if (activeFiles.length === 0) {
        return { progress: 0, totalSize: 0, downloadedSize: 0 };
    }

    let totalBytes = 0;
    let downloadedBytes = 0;

    activeFiles.forEach(file => {
        const size = typeof file.size === 'number' ? file.size : 0;
        const downloaded = file.bytes_downloaded || 0;

        totalBytes += size;
        downloadedBytes += downloaded;
    });

    const effectiveTotal = Math.max(totalBytes, downloadedBytes);

    const percentage = effectiveTotal === 0 ? 0 : (downloadedBytes / effectiveTotal) * 100;

    return {
        progress: percentage,
        totalSize: totalBytes,
        downloadedSize: downloadedBytes
    };
}

const DownloadCell = ({ 
  downloadId, 
  columnId, 
  customCell,
  className,
}: { 
  downloadId: number; 
  columnId: string; 
  customCell?: (download: DownloadItem) => React.ReactNode; 
  className?: string,
}) => {
    const download = useDownloadStore((s) => s.downloads[downloadId]);

    if (!download) return null;

    if (customCell) {
        return customCell(download);
    }

    const textValue = download[columnId as keyof DownloadItem];
    return <div className={`text-muted-foreground truncate ${className}`}>{String(textValue ?? "")}</div>;
};

const columnHelper = createColumnHelper<number>();

function createColumn({
  id,
  header,
  size,
  cell,
  className,
}: {
  id: string;
  header: React.ReactNode;
  size?: number;
  cell?: (download: DownloadItem) => React.ReactNode;
  className?: string,
}) {
  return columnHelper.display({
    id,
    header: () => header,
    size,
    cell: (info) => (
      <DownloadCell 
        downloadId={info.row.original} 
        columnId={id} 
        customCell={cell} 
        className={className}
      />
    ),
  });
}

const columns = [
    createColumn({ id: "name", header: "Name", cell: (download) =>
    {
        const isDownloading = download.status.state === "in_progress";
        return <span className={`font-medium truncate ${isDownloading ? "text-foreground" : ""}`}>{download.name}</span>;
    }}),
    createColumn({ 
        id: "status",
        header: "Status",
        size: 120,
        cell: (download) => {
            return <>{download.active_operation ? download.active_operation : formatDownloadStatus(download.status)}</>;
        }
    }),
    createColumn({ 
        id: "size", 
        header: "Size",
        size: 100, 
        className: "text-right",
        cell: (download) => {
            const stats = getFolderStats(download.files);
            return <>{formatBytes(stats.totalSize)}</>;
        }
    }),
    createColumn({
        id: "progress",
        header: "Progress",
        size: 200,
        cell: (download) => {
            const stats = getFolderStats(download.files);
            const displayVal = Math.round(stats.progress); 

            return (
                <div className="flex items-center gap-2">
                <div className={`flex-1 h-1 rounded-none overflow-hidden bg-muted`}>
                    <div className={`h-full bg-blue-500`} style={{ width: `${stats.progress}%` }} />
                </div>
                <span className="text-xs text-muted-foreground w-8 text-right">{displayVal}%</span>
                </div>
            );
        }
    }),
    createColumn({ 
        id: "speed", 
        header: "Speed", 
        size: 120, 
        className: "text-right",
        cell: (download) => <SpeedCellContent download={download} />
    }),
    createColumn({ id: "eta", header: "ETA", size: 120, className: "text-right"}),
    createColumn({ id: "limit", header: "Limit", size: 120, className: "text-right"}),
];

export function DownloadsTable({ downloadIds }: { downloadIds: number[] }) {
  const table = useReactTable({
    data: downloadIds,
    columns,
    getCoreRowModel: getCoreRowModel(),
    columnResizeMode: "onChange",
    getRowId: (originalRow) => String(originalRow)
  });
  const [isTableFocused, setTableFocused] = useState(false);
  const { selectedId, setSelectedId } = useDownloadStore();

  const { rows } = table.getRowModel();
  const tableContainerRef = useRef<HTMLDivElement>(null);

    // Table focus logic
    useEffect(() => {
        const handleClick = (event: MouseEvent) => {
            if (!tableContainerRef.current) return;

            const target = event.target as HTMLElement;

            // We ignore the click if the header was clicked
            if (target.closest("thead") || target.closest("th")) {
                return; 
            }

            if (tableContainerRef.current.contains(event.target as Node)) {
                console.log("table focused");
                setTableFocused(true);
            } else {
                setTableFocused(false);
                console.log("table not focused");
            }
        };

        // We add a global listener
        document.addEventListener("mousedown", handleClick);

        // When this component is unmounted, we remove the event listener
        return () => {
            document.removeEventListener("mousedown", handleClick);
        };
    }, []); // Only runs once when component is mounted

    // Keyboard logic (Moving through table with up and down arrows)
    useEffect(() => {
        const handleKeyDown = (event: KeyboardEvent) => {
            if (!selectedId) return;

            if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                event.preventDefault(); 

                // If nothing is selected yet, select the first item and stop
                if (!selectedId && downloadIds.length > 0) {
                    setTableFocused(true);
                    setSelectedId(downloadIds[0]);
                    return;
                }

                const currentIndex = downloadIds.indexOf(selectedId!);

                if (currentIndex === -1) return; 

                if (event.key === "ArrowDown") {
                    // Check if we are already at the bottom
                    if (currentIndex < downloadIds.length - 1) {
                        setSelectedId(downloadIds[currentIndex + 1]);
                    }
                } 
                
                else if (event.key === "ArrowUp") {
                    // Check if we are already at the top
                    if (currentIndex > 0) {
                        setSelectedId(downloadIds[currentIndex - 1]);
                    }
                }

                setTableFocused(true);
            }
        };

        document.addEventListener("keydown", handleKeyDown);

        return () => {
            document.removeEventListener("keydown", handleKeyDown);
        };
    }, [isTableFocused, selectedId, downloadIds]); 

    const rowVirtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => tableContainerRef.current,
    estimateSize: () => 53,
    overscan: 10,
  });

  const items = rowVirtualizer.getVirtualItems();
  const paddingTop = items.length > 0 ? items[0].start : 0;
  const paddingBottom = items.length > 0 ? rowVirtualizer.getTotalSize() - items[items.length - 1].end : 0;

  const columnSizeVars = useMemo(() => {
    const headers = table.getFlatHeaders();
    const colSizes: {[key: string]: number } = {};
    for (let i = 0; i < headers.length; i++) {
      const header = headers[i]!;
      colSizes[`--header-${header.id}-size`] = header.getSize();
      colSizes[`--col-${header.column.id}-size`] = header.column.getSize();
    }
    return colSizes;
  },[table.getState().columnSizingInfo, table.getState().columnSizing]);

  const columnSizingState = table.getState().columnSizing;
  
  return (
    <div ref={tableContainerRef} className="w-full overflow-auto">
      <Table 
        className="table-fixed w-full" 
        style={{ 
          ...columnSizeVars,
          minWidth: table.getTotalSize() 
        } as React.CSSProperties}
      >
        
        {/* --- HEADERS --- */}
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow 
                key={headerGroup.id}
                className="border-border hover:bg-transparent"
            >
              {headerGroup.headers.map((header, index, array) => {
                const isFirstColumn = index === 0;
                const isLastColumn = index === array.length - 1;
                
                const isTableResized = Object.keys(columnSizingState).length > 0;

                let colWidth = `calc(var(--header-${header.id}-size) * 1px)`;
                if (isFirstColumn) {
                    colWidth = isTableResized ? colWidth : "100%";
                } else if (isLastColumn) {
                    colWidth = isTableResized ? "100%" : colWidth;
                }

                return (
                    <TableHead
                    key={header.id}
                    style={{ width: colWidth }}
                    className={cn(
                        "relative text-xs select-none text-muted-foreground truncate h-7", 
                        index === 0 && "flex-1"
                    )} 
                    >
                    {flexRender(header.column.columnDef.header, header.getContext())}
                    
                    {header.column.getCanResize() && (
                        <div
                            onMouseDown={(e) => {
                                if (Object.keys(columnSizingState).length === 0) {
                                    const tr = (e.currentTarget as HTMLElement).closest("tr");
                                    const ths = tr?.querySelectorAll("th");
                                    if (ths) {
                                        array.forEach((h, i) => {
                                            if (ths[i]) {
                                                h.column.columnDef.size = ths[i].getBoundingClientRect().width;
                                            }
                                        });
                                    }
                                } else if (columnSizingState[header.column.id] === undefined) {
                                    const th = (e.currentTarget as HTMLElement).closest("th");
                                    if (th) header.column.columnDef.size = th.getBoundingClientRect().width;
                                }
                                header.getResizeHandler()(e);
                            }}
                            onTouchStart={(e) => {
                                if (Object.keys(columnSizingState).length === 0) {
                                    const tr = (e.currentTarget as HTMLElement).closest("tr");
                                    const ths = tr?.querySelectorAll("th");
                                    if (ths) {
                                        array.forEach((h, i) => {
                                            if (ths[i]) {
                                                h.column.columnDef.size = ths[i].getBoundingClientRect().width;
                                            }
                                        });
                                    }
                                } else if (columnSizingState[header.column.id] === undefined) {
                                    const th = (e.currentTarget as HTMLElement).closest("th");
                                    if (th) header.column.columnDef.size = th.getBoundingClientRect().width;
                                }
                                
                                header.getResizeHandler()(e);
                            }}
                            className="absolute right-[-4px] top-0 h-full w-2 cursor-col-resize z-10 touch-none select-none "
                        >
                            <div className="w-[1px] h-full transition-colors bg-border group-hover:bg-blue-500/50" />
                        </div>
                    )}
                    </TableHead>
                )}
              )}
            
            </TableRow>
          ))}
        </TableHeader>

        {/* --- ROWS --- */}
        <TableBody>
            {paddingTop > 0 && (
                <TableRow>
                <TableCell style={{ height: `${paddingTop}px` }} colSpan={columns.length} />
                </TableRow>
            )}

            {items.map((virtualRow) => {
                const row = rows[virtualRow.index];

            return (
                <TableRow
                    key={row.original}
                    ref={rowVirtualizer.measureElement} 
                    data-index={virtualRow.index}       
                    onClick={() => setSelectedId(row.original)}
                    className={cn(
                        `text-xs hover:bg-[#2a2d2e] transition-none`,
                        selectedId == row.original && "outline text-foreground -outline-offset-1 outline-dotted outline-[#919191] bg-background",
                        selectedId == row.original && isTableFocused && "bg-[#37373d] hover:bg-[#37373d]",
                    )}
                >
                    {row.getVisibleCells().map((cell, index, array) => {
                        const isFirstColumn = index === 0;
                        const isLastColumn = index === array.length - 1;
                        const isTableResized = Object.keys(columnSizingState).length > 0;

                        let colWidth = `calc(var(--col-${cell.column.id}-size) * 1px)`;
                        if (isFirstColumn) {
                            colWidth = isTableResized ? colWidth : "100%"; 
                        } else if (isLastColumn) {
                            colWidth = isTableResized ? "100%" : colWidth;
                        }

                        return (
                            <TableCell 
                                key={cell.id} 
                                style={{ width: colWidth }}
                                className="truncate max-w-0"
                            >
                                {flexRender(cell.column.columnDef.cell, cell.getContext())}
                            </TableCell>
                        )}
                    )}
                </TableRow>
            );
          })}

          {paddingBottom > 0 && (
            <TableRow>
              <TableCell style={{ height: `${paddingBottom}px` }} colSpan={columns.length} />
            </TableRow>
          )}
        </TableBody>

      </Table>
    </div>
  );
}