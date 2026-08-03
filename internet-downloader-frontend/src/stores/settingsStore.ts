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
    mutationFn: ({ speed_limit, default_save_path }: { speed_limit?: number | null, default_save_path?: string | null }) => {
      return fetch(`${API_BASE}/settings`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ global_speed_limit: speed_limit, default_save_path }),
      })
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
}

export function useSetHostSettings() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ host, speed_limit }: { host: string; speed_limit: number | null }) =>
      fetch(`${API_BASE}/hosts/${host}/settings`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ speed_limit }),
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
}

export function useRemoveHostSettings() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (host: string) =>
      fetch(`${API_BASE}/hosts/${host}/settings`, { method: "DELETE" }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
}
