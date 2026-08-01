import { useSettings, useSetGlobalLimit } from "@/stores/settingsStore";
import { useState } from "react";

export default function GlobalSpeedLimit() {
  const { data: settings } = useSettings();
  const setGlobalLimit = useSetGlobalLimit();

  const currentLimit = settings?.global_speed_limit ?? null;
  const displayMb = currentLimit ? (currentLimit / (1024 * 1024)).toFixed(1) : null;

  const [inputMb, setInputMb] = useState(displayMb ?? "");

  return (
    <div className="flex items-center gap-2">
      <input
        type="number"
        min="0"
        step="0.1"
        value={inputMb}
        onChange={(e) => setInputMb(e.target.value)}
        placeholder="MB/s"
        className="h-8 w-24 rounded-sm bg-[#1A1C1E] border border-border px-2 text-[13px] text-foreground focus:border-gray-500 focus:outline-none"
      />
      <span className="text-[13px] text-muted-foreground">MB/s</span>
      <button
        onClick={() => {
          const mb = parseFloat(inputMb);
          if (isNaN(mb) || mb <= 0) {
            setGlobalLimit.mutate(null);
            setInputMb("");
          } else {
            setGlobalLimit.mutate(mb * 1024 * 1024);
          }
        }}
        disabled={setGlobalLimit.isPending}
        className="h-8 px-3 rounded-sm bg-accent text-[13px] text-foreground hover:bg-accent-foreground/15 transition-colors cursor-pointer disabled:opacity-50"
      >
        Apply
      </button>
      {currentLimit && (
        <button
          onClick={() => {
            setGlobalLimit.mutate(null);
            setInputMb("");
          }}
          disabled={setGlobalLimit.isPending}
          className="h-8 px-3 rounded-sm text-[13px] text-muted-foreground hover:text-foreground transition-colors cursor-pointer disabled:opacity-50"
        >
          Remove limit
        </button>
      )}
    </div>
  );
}
