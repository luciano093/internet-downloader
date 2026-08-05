import type { AppSettings } from "@/downloadTypes";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

const API_BASE = "http://localhost:3211";

export function useSettings() {
  return useQuery<AppSettings>({
    queryKey: ["settings"],
    queryFn: () => fetch(`${API_BASE}/settings`, { cache: 'no-cache' }).then(res => res.json()),
  });
}

export function useSetGlobalSettings() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async ({ speed_limit, default_save_path }: { speed_limit?: number | null, default_save_path?: string | null }) => {
      const response = await fetch(`${API_BASE}/settings`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ global_speed_limit: speed_limit, default_save_path }),
      });

      if (!response.ok) throw new Error(`Failed to update settings (${response.status})`);
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
}

export function useSetDefaultSavePath() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: async (default_save_path: string | null) => {
      const response = await fetch(`${API_BASE}/settings`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ default_save_path }),
      });

      if (!response.ok) throw new Error(`Failed to update default save path (${response.status})`);
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
}

export function useSetHostSettings() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ host, speed_limit }: { host: string; speed_limit: number | null }) => {
      const response = await fetch(`${API_BASE}/hosts/${encodeURIComponent(host)}/settings`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ speed_limit }),
      });

      if (!response.ok) throw new Error(`Failed to update host settings (${response.status})`);
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
}

export function useRemoveHostSettings() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (host: string) => {
      const response = await fetch(`${API_BASE}/hosts/${encodeURIComponent(host)}/settings`, { method: "DELETE" });

      if (!response.ok) throw new Error(`Failed to remove host settings (${response.status})`);
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
}
