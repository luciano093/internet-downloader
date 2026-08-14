import { useQuery } from "@tanstack/react-query";

const API_BASE = "http://localhost:3211";

type SavePathError = {
  state: string;
  message: string;
};

type PathValidation =
  | { status: "idle" } // we received an empty path, so there is nothing to check/validate
  | { status: "error_validating" }
  | { status: "validating" }
  | { status: "valid" }
  | { status: "invalid"; message: string };

export default function usePathValidation(path: string): PathValidation {
  const trimmed_path = path.trim();

  const query = useQuery({
    queryKey: ["validate-path", trimmed_path],
    queryFn: async (): Promise<SavePathError | null> => {
      const response = await fetch(`${API_BASE}/validate-path`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path: trimmed_path }),
      });

      if (!response.ok) {
        throw new Error(`Validation failed (${response.status})`);
      }

      return await response.json();
    },
    enabled: trimmed_path !== "",
    staleTime: 60_000,   // cache an unchanged path for a minute
  });

  if (trimmed_path === "") {
    return { status: "idle" };
  }

  if (query.isError) {
    // network/logic failure
    return { status: "error_validating" };
  }

  if (query.isPending || query.isFetching) {
    return { status: "validating" };
  }

  if (query.data === null) {
    return { status: "valid" };
  }

  if (query.data) {
    return { status: "invalid", message: query.data.message };
  }

  return { status: "validating" };
}
