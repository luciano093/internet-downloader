import type { ReactNode } from "react";

interface TopBarButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  icon?: ReactNode;
  label: string;
}

export function TopBarButton({ icon, label, ...props }: TopBarButtonProps) {
  return (
    <button
      {...props}
      className="flex items-center gap-1.5 pl-1.5 pr-2 h-full text-xs whitespace-nowrap text-foreground transition-colors hover:bg-white/10 hover:text-accent-foreground disabled:opacity-50 cursor-pointer"
    >
      {icon && <span>{icon}</span>}
      <span>{label}</span>
    </button>
  );
}