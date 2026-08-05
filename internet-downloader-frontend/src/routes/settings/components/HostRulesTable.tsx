import { useSettings, useSetHostSettings, useRemoveHostSettings } from '@/stores/settingsStore';
import { useEffect, useRef, useState } from 'react';
import EditableBytesLimit, { parseLimit } from '@/components/EditableLimit';
import { LimitInput } from '@/components/LimitInput';

interface AddHostRuleRowProps {
  onAdd: (host: string, speed_limit: number | null) => void,
  onCancel: () => void,
  isPending?: boolean,
  error?: string,
}

function AddHostRuleRow({ onAdd, onCancel, isPending, error }: AddHostRuleRowProps) {
  const addRowRef = useRef<HTMLTableRowElement>(null);
  const [newHostName, setNewHostName] = useState("");
  const [newHostLimit, setNewHostLimit] = useState("");

  const commitAddHost = () => {
    if (!newHostName.trim()) {
      cancelAddHost();
      return;
    }
    
    const limit = parseLimit(newHostLimit, 1024 * 1024);
    onAdd(newHostName, limit);
    cancelAddHost();
  }

  const cancelAddHost = () => {
    onCancel();
    setNewHostName("");
    setNewHostLimit("");
  };

  const handleAddKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === 'Enter') commitAddHost();
    if (event.key === 'Escape') cancelAddHost();
  };

  const commitRef = useRef(commitAddHost);
  commitRef.current = commitAddHost;

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (addRowRef.current && !addRowRef.current.contains(event.target as Node)) {
        commitRef.current();
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  return <>
    {error && (
      <tr>
        <td colSpan={3} className="py-1 px-3 text-[11px] text-destructive">
          {error}
        </td>
      </tr>
    )}
    <tr ref={addRowRef} className="border-t border-border">
      <td className="py-1.5 px-3 border-r border-border">
        <input
          type="text"
          placeholder="e.g. github.com"
          value={newHostName}
          onChange={(event) => setNewHostName(event.target.value)}
          autoFocus
          className="w-full bg-background border border-border focus:border-brand text-foreground outline-none px-2 py-0.5 text-xs font-mono"
          onKeyDown={handleAddKeyDown}
          disabled={isPending}
        />
      </td>
      <td className="py-1.5 px-3 text-right">
        <LimitInput
          value={newHostLimit}
          onValueChange={setNewHostLimit}
          onEnter={commitAddHost}
          onEscape={cancelAddHost}
          placeholder="∞"
          unit="MB/s"
        />
      </td>
    </tr>
  </>
}

function HostRuleRow({ host, speedLimit, onCommit, onDelete, isDeleting, deleteError }: {
  host: string;
  speedLimit: number | null;
  onCommit: (host: string, speedLimit: number | null) => void;
  onDelete: (host: string) => void;
  isDeleting?: boolean;
  deleteError?: string;
}) {
  const [isEditing, setIsEditing] = useState(false);

  return (
    <tr key={host} className="group border-t border-border hover:text-foreground hover:bg-accent">
      <td className="py-1.5 px-3 border-r border-border text-foreground">
        {host}
        {deleteError && (
          <span className="ml-2 text-[11px] text-destructive">Failed to remove</span>
        )}
      </td>
      <td className="py-1.5 px-3 text-right border-r border-border">
        <EditableBytesLimit
          value={speedLimit}
          editing={isEditing}
          onEditingChange={setIsEditing}
          editingUnit="MB/s"
          bytesPerUnit={1024 * 1024}
          onCommit={(bytes) => onCommit(host, bytes)}
          commitOnBlur
      />
      </td>
      <td className='text-center'>
        <button
          onClick={() => onDelete(host)}
          className="opacity-0 group-hover:opacity-100 focus-visible:opacity-100 text-muted hover:text-accent-foreground text-xs cursor-pointer"
          title="Remove rule"
        >
          {isDeleting ? "..." : "x"}
        </button>
      </td>
    </tr>
  );
}

export function HostRulesTable() {
  const { data: settings } = useSettings();
  const setHostLimit = useSetHostSettings();
  const removeHostLimit = useRemoveHostSettings();
  
  const [addingHost, setAddingHost] = useState(false);

  const hostSettings = settings?.host_settings ?? {};

  const onAdd = (host: string, speed_limit: number | null) => {
    setHostLimit.mutate({ host, speed_limit });
  };
  
  const onCancel = () => {
    setAddingHost(false);
  };

  const onDelete = (host: string) => {
    removeHostLimit.mutate(host);
  };

  return (
    <section>
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-foreground font-semibold">Per-Host Rules</h3>
        <button
          onClick={() => setAddingHost(true)}
          className="text-brand hover:opacity-80 flex items-center gap-1 text-xs cursor-pointer"
        >
          + Add Host Rule
        </button>
      </div>

      <table className="w-full text-left border-collapse border border-border">
        <thead className="bg-sidebar text-foreground text-[11px] uppercase">
          <tr>
            <th className="font-normal py-1.5 px-3 border-r border-border">
              Hostname
            </th>
            <th className="font-normal py-1.5 px-3 w-32 text-right border-r border-border">
              Limit
            </th>
            <th className="font-normal px-3 w-3 text-center">
              {/* Remove button */}
            </th>
          </tr>
        </thead>
        <tbody>
          {Object.keys(hostSettings).length === 0 && !addingHost && (
            <tr>
              <td colSpan={3} className="py-4 text-center text-muted">
                No host rules defined.
              </td>
            </tr>
          )}

          {addingHost && (
            <AddHostRuleRow
              onAdd={onAdd}
              onCancel={onCancel}
              isPending={setHostLimit.isPending}
              error={setHostLimit.error?.message}
            />
          )}
          
          {Object.entries(hostSettings).map(([host, hostSetting]) => (
            <HostRuleRow
              key={host}
              host={host}
              speedLimit={hostSetting.speed_limit}
              onCommit={onAdd}
              onDelete={onDelete}
              isDeleting={removeHostLimit.isPending && removeHostLimit.variables === host}
              deleteError={removeHostLimit.isError && removeHostLimit.variables === host ? removeHostLimit.error?.message : undefined}
            />
          ))}
        </tbody>
      </table>
      <p className="text-[11px] text-muted mt-2">
        Host limits override the global limit.
      </p>
    </section>
  );
}
