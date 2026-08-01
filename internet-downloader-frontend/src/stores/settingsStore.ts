import type { AppSettings } from "@/downloadTypes";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

const API_BASE = "http://localhost:3211";

export function useSettings() {
  return useQuery<AppSettings>({
    queryKey: ["settings"],
    queryFn: () => fetch(`${API_BASE}/settings`, { cache: 'no-cache' }).then(res => res.json()),
  });
}

export function useSetGlobalLimit() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: (bandwidth_limit: number | null) => {
      return fetch(`${API_BASE}/limit`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ bandwidth_limit }),
      })
    },
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
}

export function useSetHostLimit() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ host, bandwidth_limit }: { host: string; bandwidth_limit: number | null }) =>
      fetch(`${API_BASE}/hosts/${host}/limit`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ host, bandwidth_limit }),
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["settings"] }),
  });
}
