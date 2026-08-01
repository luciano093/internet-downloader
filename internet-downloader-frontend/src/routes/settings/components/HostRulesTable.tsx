import { useSettings, useSetHostLimit } from '@/stores/settingsStore';
import { useEffect, useRef, useState } from 'react';
import { formatLimit } from '../lib/utils';
import EditableBytesLimit from '@/components/EditableLimit';

export function HostRulesTable() {
  const { data: settings } = useSettings();
  const setHostLimit = useSetHostLimit();

  const [editingHost, setEditingHost] = useState<string | null>(null);
  const [addingHost, setAddingHost] = useState(false);
  const [newHostName, setNewHostName] = useState("");
  const [newHostLimit, setNewHostLimit] = useState("");

  const addRowRef = useRef<HTMLTableRowElement>(null);

  const hostSettings = settings?.host_settings ?? {};

  useEffect(() => {
    if (!addingHost) return;

    const handleClickOutside = (e: MouseEvent) => {
      if (addRowRef.current && !addRowRef.current.contains(e.target as Node)) {
        if (!newHostName.trim()) {
          // no host typed = just cancel
          setAddingHost(false);
          setNewHostName("");
          setNewHostLimit("");
          return;
        }
        // host typed = commit
        const mb = parseFloat(newHostLimit);
        const limit = isNaN(mb) || mb <= 0 ? null : Math.round(mb * 1024 * 1024);
        setHostLimit.mutate({ host: newHostName, bandwidth_limit: limit });
        setAddingHost(false);
        setNewHostName("");
        setNewHostLimit("");
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [addingHost, newHostName, newHostLimit]);

  return (
    <section>
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-foreground font-semibold">Per-Host Rules</h3>
        <button
          onClick={() => setAddingHost(true)}
          className="text-brand hover:opacity-80 flex items-center gap-1 text-xs"
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
            <th className="font-normal py-1.5 px-3 w-32 text-right">
              Limit
            </th>
          </tr>
        </thead>
        <tbody>
          {Object.keys(hostSettings).length === 0 && !addingHost && (
            <tr>
              <td colSpan={2} className="py-4 text-center text-muted">
                No host rules defined.
              </td>
            </tr>
          )}

          {addingHost && (
            <tr ref={addRowRef} className="border-t border-border">
              <td className="py-1.5 px-3 border-r border-border">
                <input
                  type="text"
                  placeholder="e.g. github.com"
                  value={newHostName}
                  onChange={(e) => setNewHostName(e.target.value)}
                  autoFocus
                  className="w-full bg-background border border-border focus:border-brand text-foreground outline-none px-2 py-0.5 text-xs font-mono"
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      if (!newHostName.trim()) {
                        // blank hostname = cancel
                        setAddingHost(false);
                        setNewHostName("");
                        setNewHostLimit("");
                        return;
                      }
                      const mb = parseFloat(newHostLimit);
                      const limit = isNaN(mb) || mb <= 0 ? null : Math.round(mb * 1024 * 1024);
                      setHostLimit.mutate({ host: newHostName, bandwidth_limit: limit });
                      setAddingHost(false);
                      setNewHostName("");
                      setNewHostLimit("");
                    }
                    if (e.key === 'Escape') {
                      setAddingHost(false);
                      setNewHostName("");
                      setNewHostLimit("");
                    }
                  }}
                />
              </td>
              <td className="py-1.5 px-3 text-right">
                <div className="inline-flex items-center gap-1.5 whitespace-nowrap justify-end">
                  <input
                    type="number"
                    min="0"
                    step="0.1"
                    placeholder="∞"
                    value={newHostLimit}
                    onChange={(e) => setNewHostLimit(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        if (!newHostName.trim()) {
                          setAddingHost(false);
                          setNewHostName("");
                          setNewHostLimit("");
                          return;
                        }
                        
                        const mb = parseFloat(newHostLimit);
                        const limit = isNaN(mb) || mb <= 0 ? null : Math.round(mb * 1024 * 1024);
                        setHostLimit.mutate({ host: newHostName, bandwidth_limit: limit });
                        setAddingHost(false);
                        setNewHostName("");
                        setNewHostLimit("");
                      }
                      if (e.key === 'Escape') {
                        setAddingHost(false);
                        setNewHostName("");
                        setNewHostLimit("");
                      }
                    }}
                    className="w-20 bg-background border border-border focus:border-brand text-foreground outline-none px-2 py-0.5 text-xs font-mono"
                  />
                  <span className="text-muted text-[11px]">MB/s</span>
                </div>
              </td>
            </tr>
          )}
          
          {Object.entries(hostSettings).map(([host, hostSetting]) => (
            <tr key={host} className="border-t border-border hover:bg-[#2a2d2e]">
              <td className="py-1.5 px-3 border-r border-border text-foreground">
                {host}
              </td>
              <td className="py-1.5 px-3 text-right">
                {editingHost === host ? (
                  <EditableBytesLimit
                    value={hostSetting.speed_limit}
                    editing={editingHost === host}
                    onEditingChange={(nowEditing) => setEditingHost(nowEditing ? host : null)}
                    editingUnit="MB/s"
                    bytesPerUnit={1024 * 1024}
                    onCommit={(bytes) => {
                      setHostLimit.mutate({ host, bandwidth_limit: bytes });
                      setEditingHost(null);
                    }}
                    commitOnBlur
                  />
                ) : (
                  <span onClick={() => setEditingHost(host)}>
                    {formatLimit(hostSetting.speed_limit)}
                  </span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="text-[11px] text-muted mt-2">
        Host limits override the global limit.
      </p>
    </section>
  );
}
