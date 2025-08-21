import type { ReactNode } from "react";

export function DownloadRow({ children, className = "" }: { children: ReactNode, className?: string }) {
  return (
    <div className={`grid grid-cols-3 group ${className}`}>
      {children}
    </div>
  );
}

export function DownloadCell({ title, children }: { 
  title?: string, 
  children?: ReactNode,
  isHeader?: boolean 
}) {
  return (
    <div>
      {title && (
        <div className={`px-2 py-1 border-r border-b border-gray-800`}>
          {title}
        </div>
      )}
      <div className="border-r border-gray-800 group-hover:[&_*]:bg-gray-400 group-hover:[&_*]:text-gray-800 group-hover:[&_*]:cursor-default">
        {children}
      </div>
    </div>
  );
}

export function DownloadGrid({ children }: { children: ReactNode }) {
  return (
    <div className="bg-gray-800 border border-gray-800">
      {children}
    </div>
  );
}