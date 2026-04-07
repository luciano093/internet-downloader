/// <reference types="vite/client" />
import { Outlet, createRootRoute } from '@tanstack/react-router'
import '../index.css';

export const Route = createRootRoute({
  component: RootComponent,
})

function RootComponent() {
  return (
    <div className="min-h-screen">
      <Outlet />
    </div>
  );
}