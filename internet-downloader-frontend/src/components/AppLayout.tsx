import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable";
import { SidebarItem } from "./SidebarItem";
import { ArrowDownToLine, Settings } from "lucide-react";
import { useEffect, useRef } from "react";
import { useLocation, useRouter } from "@tanstack/react-router";
import { useUiStore } from "@/stores/uiStore";

interface AppLayoutProps {
  topBar?: React.ReactNode;
  sidebarTop?: React.ReactNode;
  bottomPane?: React.ReactNode;
  children: React.ReactNode;
}

export default function AppLayout({ topBar, sidebarTop, bottomPane, children }: AppLayoutProps) {
  const sidebarRef = useRef<HTMLDivElement>(null);
  const isResizing = useRef(false);
  const router = useRouter();
  const location = useLocation();

  // sizes
  const sidebarWidth = useUiStore(store => store.sidebarWidth);
  const setSidebarWidth = useUiStore(store => store.setSidebarWidth);
  const sidebarTopSize = useUiStore(store => store.sidebarTopPercentage);
  const setSidebarTopSize = useUiStore(store => store.setSidebarTopPercentage);
  const bottomPaneSize = useUiStore(store => store.bottomPaneSize);
  const setBottomPaneSize = useUiStore(store => store.setBottomPaneSize);

    useEffect(() => {
        const handleMouseMove = (e: MouseEvent) => {
            if (!isResizing.current) return;
            
            requestAnimationFrame(() => {
                if (sidebarRef.current) {
                    const newWidth = Math.min(Math.max(e.clientX, 100), window.innerWidth * 0.9);
                    sidebarRef.current.style.width = `${newWidth}px`;
                }
            });
        };
      
        const handleMouseUp = () => {
          if (!isResizing.current) return;
          
          isResizing.current = false;
          document.body.style.cursor = "default";
          document.body.style.userSelect = "auto";
          const width = sidebarRef.current?.style.width;
          
          if (width) {
            setSidebarWidth(parseInt(width));
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
                    style={{ width: `${sidebarWidth}px`, flexShrink: 0 }} 
                    className="bg-sidebar flex flex-col h-full"
                >
                  <ResizablePanelGroup
                    orientation="vertical"
                    id="sidebar-vertical"
                    defaultLayout={{
                      "sidebar-top": sidebarTopSize,
                      "sidebar-nav": 100 - sidebarTopSize,
                    }}
                    onLayoutChanged={(layout) => {
                      setSidebarTopSize(layout["sidebar-top"]);
                    }}
                  >
                    
                    {/* Dynamic Sidebar (Top of sidebar) */}
                    <ResizablePanel id="sidebar-top" minSize={10}>
                        <div className="flex-1 overflow-y-auto overflow-x-hidden h-full p-2">
                        {sidebarTop || <div className="text-muted-foreground p-2">Top Content</div>}
                        </div>
                    </ResizablePanel>

                    {/* Vertical Split Handle */}
                    <ResizableHandle />
                    
                    {/* Global Views Navigation (Bottom of sidebar) */}
                    <ResizablePanel id="sidebar-nav" minSize={10}>
                        <div className="h-full flex flex-col pt-2">
                        <div className="text-xs font-semibold text-muted-foreground mb-2 px-4">VIEWS</div>
                        <SidebarItem 
                          icon={ArrowDownToLine} 
                          label="Downloads" 
                          isActive={location.pathname === '/'} 
                          onClick={() => router.navigate({ to: '/' })}
                          onMouseEnter={() => router.preloadRoute({ to: '/' })}
                        />
                        <SidebarItem 
                          icon={Settings} 
                          label="Settings" 
                          isActive={location.pathname === '/settings'} 
                          onClick={() => router.navigate({ to: '/settings' })}
                          onMouseEnter={() => router.preloadRoute({ to: '/settings' })}
                        />
                        </div>
                    </ResizablePanel>

                    </ResizablePanelGroup>
                </div>

                <div
                    onMouseDown={handleMouseDown}
                    className="relative w-2 -ml-1 -mr-1 cursor-col-resize group z-10 flex items-center justify-center">
                    <div className="absolute inset-y-0 w-4" />
                    <div className="w-[1px] h-full bg-accent" />
                </div>

                {/* Main content */}
                <div className="bg-background flex flex-col flex-1 min-w-0">
                  <ResizablePanelGroup
                    orientation="vertical"
                    id="main-bottom"
                    defaultLayout={{
                      "main-content": 100 - bottomPaneSize,
                      "bottom-pane": bottomPaneSize,
                    }}
                    onLayoutChanged={(layout) => {
                      if (bottomPane && layout["bottom-pane"] !== undefined) {
                        setBottomPaneSize(layout["bottom-pane"]);
                      }
                    }}
                  >
                    <ResizablePanel id="main-content" minSize={20}>
                        {children}
                      </ResizablePanel>
                    {
                      bottomPane && <>
                        <ResizableHandle className="bg-border" />
                        <ResizablePanel id="bottom-pane" minSize={10}>
                          {bottomPane}
                        </ResizablePanel>
                      </>
                    }
                  </ResizablePanelGroup>
                </div>
            </div>
        </div>
    </>
}
