import AppLayout from '@/components/AppLayout';
import { createFileRoute } from '@tanstack/react-router'
import { useSettings, useSetGlobalSettings } from '@/stores/settingsStore';
import { useState } from 'react';
import { HostRulesTable } from './components/HostRulesTable';
import EditableBytesLimit from '@/components/EditableLimit';
import SettingsSidebar from './components/SettingsSidebar';

export const Route = createFileRoute('/settings/')({
  component: Settings,
})

function Settings() {
  const { data: settings } = useSettings();
  const setGlobalSettings = useSetGlobalSettings();

  const [savePath, setSavePath] = useState(settings?.default_save_path);
  const [editing, setEditing] = useState(false);

  const globalLimit = settings?.global_speed_limit ?? null;

  return (
    <AppLayout sidebarTop={<SettingsSidebar />}>
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
                  onCommit={(bytes) => setGlobalSettings.mutate({ speed_limit: bytes })}
                  commitOnBlur />
              </div>
              <p className="text-[11px] text-muted mt-1">
                {globalLimit ? "Click the value to change or remove the limit." : "Click the value to set a global download speed limit."}
              </p>
            </div>
          </section>

          <section>
            <h3 className="text-foreground font-semibold mb-3">Storage</h3>
            <div className="bg-sidebar border border-border p-3 rounded-sm">
              <div className="flex justify-between items-center">
                <span className="text-foreground">Default Save Path</span>
                <div className="flex items-center gap-1.5">
                  <input
                    type="text"
                    value={savePath ?? ""}
                    onChange={(event) => setSavePath(event.target.value)}
                    placeholder="/downloads/completed/"
                    className="w-48 bg-background border border-border focus:border-brand text-foreground outline-none px-2 py-0.5 rounded-sm text-xs font-mono"
                  />
                  <button
                    onClick={() => setGlobalSettings.mutate({ default_save_path: savePath })}
                    className="text-brand hover:opacity-80 text-[11px] cursor-pointer"
                  >
                    Save
                  </button>
                </div>
              </div>
              <p className="text-[11px] text-muted mt-1">Default location where files will be downloaded.</p>
            </div>
          </section>

        <HostRulesTable />

        </div>
      </div>
    </AppLayout>
  );
}
