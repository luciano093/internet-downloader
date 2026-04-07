/// <reference types="vite/client" />
import { Outlet, createRootRoute } from '@tanstack/react-router'
import '../index.css';
import { AddDownloadModal } from '@/components/AddDownloadModal';

export const Route = createRootRoute({
  component: RootComponent,
})

function RootComponent() {
  return (
    <div className="min-h-screen">
      <Outlet />
      <AddDownloadModal />
    </div>
  );
}