import AppLayout from '@/components/AppLayout';
import { createFileRoute } from '@tanstack/react-router'
import { useUiStore } from '@/stores/uiStore';
import { useSettings, useSetGlobalLimit } from '@/stores/settingsStore';
import { useState } from 'react';
import { HostRulesTable } from './components/HostRulesTable';
import EditableBytesLimit from '@/components/EditableLimit';

export const Route = createFileRoute('/settings/')({
  component: Settings,
})

function Settings() {
  const { data: settings } = useSettings();
  const setGlobalLimit = useSetGlobalLimit();
  const [editing, setEditing] = useState(false);

  const globalLimit = settings?.global_speed_limit ?? null;

  return (
    <AppLayout>
      <div className="flex-1 overflow-auto bg-background">
        <div className="max-w-4xl mx-auto p-6 space-y-8 text-xs">
          
          <div>
            <h2 className="text-lg font-medium text-foreground mb-6 pb-2 border-b border-border">Settings</h2>
          </div>

          <section>
            <h3 className="text-foreground font-semibold mb-3">Bandwidth Limits</h3>
            <div className="bg-sidebar border border-border p-3 rounded-sm">
              <div className="flex justify-between items-center">
                <span className="text-foreground">Global Bandwidth Limit</span>
                <EditableBytesLimit
                  value={globalLimit}
                  editing={editing}
                  onEditingChange={setEditing}
                  editingUnit='MB/s'
                  bytesPerUnit={1024 * 1024}
                  onCommit={(bytes) => setGlobalLimit.mutate(bytes)}
                  commitOnBlur />
              </div>
              <p className="text-[11px] text-muted mt-1">
                {globalLimit ? "Click the value to change or remove the limit." : "Click the value to set a global download speed limit."}
              </p>
            </div>
          </section>

        <HostRulesTable />

        </div>
      </div>
    </AppLayout>
  );
}
