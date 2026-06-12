import AppLayout from '@/components/AppLayout';
import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/settings/')({
  component: Settings,
})

function Settings() {
  return (
    <AppLayout>
      <div className="p-6">
        <h1 className="text-lg font-medium mb-4">Settings</h1>
      </div>
    </AppLayout>
  );
}
