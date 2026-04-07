import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable";
import { SidebarItem } from "./SidebarItem";
import { ArrowDownToLine, Settings } from "lucide-react";
import { useEffect, useRef } from "react";

interface AppLayoutProps {
  topBar?: React.ReactNode;
  sidebarTop?: React.ReactNode;
  children: React.ReactNode;
}

export default function AppLayout({ topBar, sidebarTop, children }: AppLayoutProps) {
    const sidebarRef = useRef<HTMLDivElement>(null);
    const isResizing = useRef(false);

    useEffect(() => {
        const handleMouseMove = (e: MouseEvent) => {
            if (!isResizing.current) return;
            
            requestAnimationFrame(() => {
                if (sidebarRef.current) {
                    const newWidth = Math.min(Math.max(e.clientX, 100), 600);
                    sidebarRef.current.style.width = `${newWidth}px`;
                }
            });
        };

        const handleMouseUp = () => {
        if (isResizing.current) {
            isResizing.current = false;
            document.body.style.cursor = "default";
            document.body.style.userSelect = "auto";
        }
        };

        window.addEventListener("mousemove", handleMouseMove);
        window.addEventListener("mouseup", handleMouseUp);
        
        return () => {
        window.removeEventListener("mousemove", handleMouseMove);
        window.removeEventListener("mouseup", handleMouseUp);
        };
    },[]);

    const handleMouseDown = (e: React.MouseEvent) => {
        e.preventDefault();
        isResizing.current = true;
        document.body.style.cursor = "col-resize";
        document.body.style.userSelect = "none";
    };

    return <>
        <div className="flex flex-col h-screen w-screen overflow-hidden bg-background text-foreground">
            {/* Top bar */}
            <div className="h-9 flex flex-none items-center px-4 gap-4 bg-header">
                <div className="flex-1 flex items-center h-full">
                    {topBar}
                </div>
            </div>

            {/* Main app body (Sidebar + Main content) */}
            <div className="flex flex-1 overflow-hidden">
                <div 
                    ref={sidebarRef}
                    style={{ width: `200px`, flexShrink: 0 }} 
                    className="bg-sidebar flex flex-col h-full border-r border-border"
                >
                    <ResizablePanelGroup orientation="vertical">
                    
                    {/* Dynamic Sidebar (Top of sidebar) */}
                    <ResizablePanel defaultSize={80} minSize={10}>
                        <div className="flex-1 overflow-y-auto overflow-x-hidden h-full p-2">
                        {sidebarTop || <div className="text-muted-foreground p-2">Top Content</div>}
                        </div>
                    </ResizablePanel>

                    {/* Vertical Split Handle */}
                    <ResizableHandle />
                    
                    {/* Global Views Navigation (Bottom of sidebar) */}
                    <ResizablePanel defaultSize={20} minSize={10}>
                        <div className="h-full flex flex-col pt-2">
                        <div className="text-xs font-semibold text-muted-foreground mb-2 px-4">VIEWS</div>
                        <SidebarItem icon={ArrowDownToLine} label="Downloads" isActive={true} />
                        <SidebarItem icon={Settings} label="Settings" />
                        </div>
                    </ResizablePanel>

                    </ResizablePanelGroup>
                </div>

                <div
                    onMouseDown={handleMouseDown}
                    className="relative w-2 -ml-1 cursor-col-resize group z-10 flex items-center justify-center">
                    <div className="absolute inset-y-0 w-4" />
                    <div className="w-[1px] h-full bg-accent" />
                </div>

                {/* Main content */}
                <div className="bg-background flex flex-col">
                    {children}
                </div>
            </div>
        </div>
    </>
}