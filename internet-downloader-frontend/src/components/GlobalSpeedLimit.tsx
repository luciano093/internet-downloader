import { useSettings, useSetGlobalSettings } from "@/stores/settingsStore";
import { Globe } from "lucide-react";
import { useState } from "react";
import EditableBytesLimit from "./EditableLimit";

export default function GlobalSpeedLimit() {
  const { data: settings } = useSettings();
  const setGlobalSettings = useSetGlobalSettings();
  const [editing, setEditing] = useState(false);

  const globalSpeedLimit = settings?.global_speed_limit ?? null;
  
  return (
    <div className="flex items-center gap-2">
      <Globe className="h-4 w-4 text-gray-500" />
      <EditableBytesLimit
        value={globalSpeedLimit}
        editing={editing}
        onEditingChange={setEditing}
        editingUnit="MB/s"
        bytesPerUnit={1024 * 1024}
        onCommit={(bytes) => setGlobalSettings.mutate({ speed_limit: bytes })}
        commitOnBlur
        displayClassName="text-[13px] text-muted-foreground hover:text-foreground"
      />
    </div>
  );
}
