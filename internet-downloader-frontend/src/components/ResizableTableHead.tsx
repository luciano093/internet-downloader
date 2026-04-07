import { useState, useRef } from "react";
import { TableHead } from "@/components/ui/table";

interface ResizableTableHeadProps {
  children: React.ReactNode;
  defaultWidth?: number;
  className?: string;
}

export function ResizableTableHead({ children, defaultWidth, className }: ResizableTableHeadProps) {
  const [width, setWidth] = useState(defaultWidth);
  const isResizing = useRef(false);
  const thRef = useRef<HTMLTableCellElement>(null);

  const onMouseDown = (e: React.MouseEvent) => {
    e.preventDefault(); // STOPS text selection while dragging!
    if (!thRef.current) return;
    
    isResizing.current = true;
    const startX = e.pageX;
    // If width isn't set, grab the actual computed width from the DOM
    const startWidth = width || thRef.current.getBoundingClientRect().width;

    const onMouseMove = (moveEvent: MouseEvent) => {
      if (!isResizing.current) return;
      // requestAnimationFrame forces the browser to paint smoothly
      requestAnimationFrame(() => {
        setWidth(Math.max(40, startWidth + (moveEvent.pageX - startX)));
      });
    };

    const onMouseUp = () => {
      isResizing.current = false;
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", onMouseUp);
      document.body.style.cursor = "default"; // Cleanup cursor
    };

    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
    document.body.style.cursor = "col-resize"; // Force cursor everywhere during drag
  };

  return (
    <TableHead 
      ref={thRef} 
      style={
        // THE IRON FIST: Forcing min and max width stops other columns from warping!
        width ? { width: `${width}px`, minWidth: `${width}px`, maxWidth: `${width}px` } : undefined
      } 
      className={`relative group ${className || ""}`}
    >
      <div className="truncate w-full h-full flex items-center">
        {children}
      </div>

      {/* BIGGER HIT AREA: 16px wide so your mouse doesn't slip off, centered over the border */}
      <div
        onMouseDown={onMouseDown}
        className="absolute right-[-8px] top-0 h-full w-4 cursor-col-resize z-10 flex justify-center"
      >
        {/* The actual visible line (only shows on hover) */}
        <div className="w-[2px] h-full bg-brand/50 opacity-0 group-hover:opacity-100 transition-opacity" />
      </div>
    </TableHead>
  );
}