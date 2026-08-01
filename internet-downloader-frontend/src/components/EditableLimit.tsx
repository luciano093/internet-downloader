import { formatLimit } from "@/routes/settings/lib/utils";
import { useState } from "react";
import { LimitInput } from "./LimitInput";

interface EditableBytesLimitProps {
  value: number | null,
  editing: boolean;
  onEditingChange: (editing: boolean) => void;
  onCommit: (limit: number | null) => void,
  commitOnBlur?: boolean,
  placeholder?: string,
  editingUnit: string,
  bytesPerUnit?: number,
}

export default function EditableBytesLimit({ value, editing, onEditingChange, onCommit, commitOnBlur: commitOnBlur, placeholder, editingUnit, bytesPerUnit = 1 }: EditableBytesLimitProps) {
  const [draft, setDraft] = useState<string>("");
  console.log(value)

  const start = () => {
    setDraft(value == null ? "" : String(value / bytesPerUnit));
    onEditingChange(true);
  };

  const commit = () => {
    onEditingChange(false);
    onCommit(parseLimit(draft, bytesPerUnit));
  };

  const cancel = () => {
    onEditingChange(false);
  };

  if (!editing) {
    return (
      <span
        className="font-mono text-brand cursor-pointer hover:bg-accent px-1 py-0.5"
        onClick={start}
      >
        {formatLimit(value)}
      </span>
    );
  }

  return (
    <LimitInput
      value={draft}
      onValueChange={setDraft}
      onEnter={commit}
      onEscape={cancel}
      onBlur={commitOnBlur ? commit : undefined}
      autoFocus
      placeholder={placeholder}
      unit={editingUnit}
    />
  );
}

export const parseLimit = (raw: string, bytesPerUnit: number): number | null => {
  const parsed = parseFloat(raw);
  if (isNaN(parsed) || parsed < 0) return null;
  
  return parsed * bytesPerUnit;
};
