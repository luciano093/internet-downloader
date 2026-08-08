import type { RangeSelectionManager } from "@/lib/selectionManager";
import { useSyncExternalStore } from "react";

export default function useSelection<T>(selectionManager: RangeSelectionManager, fn: (selectionManager: RangeSelectionManager) => T): T {
  return useSyncExternalStore(
    (callback) => selectionManager.subscribe(callback),
    () => fn(selectionManager)
  );
}
