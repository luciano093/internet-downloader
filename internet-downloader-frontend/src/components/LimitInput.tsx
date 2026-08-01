interface LimitInputProps {
  value: string;
  onValueChange: (value: string) => void;
  onEnter?: () => void;
  onEscape?: () => void;
  onBlur?: () => void;
  autoFocus?: boolean;
  placeholder?: string;
  unit?: string;
  className?: string;
}

export function LimitInput({
  value,
  onValueChange,
  onEnter,
  onEscape,
  onBlur,
  autoFocus,
  placeholder,
  unit = "MB/s",
  className = "w-20",
}: LimitInputProps) {
  return (
    <div className="inline-flex items-center gap-1.5 whitespace-nowrap">
      <input
        type="number"
        min="0"
        step="0.1"
        value={value}
        placeholder={placeholder}
        autoFocus={autoFocus}
        onChange={(event) => onValueChange(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === 'Enter') onEnter?.();
          if (event.key === 'Escape') onEscape?.();
        }}
        onBlur={onBlur}
        className={`${className} bg-background border border-border focus:border-brand text-foreground outline-none px-2 py-0.5 text-xs font-mono`}
      />
      <span className="text-muted text-[11px]">{unit}</span>
    </div>
  );
}
