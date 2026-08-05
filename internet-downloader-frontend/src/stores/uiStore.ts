import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export type ModalType = 'add' | 'remove' | null;

interface UiState {
  activeModal: ModalType;
  openModal: (modal: ModalType) => void;
  closeModal: () => void;

  // App layout sizes (so they are persisted even when changing pages)
  sidebarWidth: number;
  setSidebarWidth: (width: number) => void;
  sidebarTopPercentage: number;
  setSidebarTopPercentage: (percent: number) => void;
  bottomPaneSize: number;
  setBottomPaneSize: (percent: number) => void;
}

export const useUiStore = create<UiState>()(
  persist(
    (set) => ({
      activeModal: null,
      openModal: (modal) => set({ activeModal: modal }),
      closeModal: () => set({ activeModal: null }),
      sidebarWidth: 200,
      setSidebarWidth: (width) => set({ sidebarWidth: width }),
      sidebarTopPercentage: 80,
      setSidebarTopPercentage: (percent) => set({ sidebarTopPercentage: percent }),
      bottomPaneSize: 40,
      setBottomPaneSize: (height) => set({ bottomPaneSize: height }),
    }),
    {
      name: 'ui-layout',
      partialize: ({ sidebarWidth, sidebarTopPercentage, bottomPaneSize }) => ({
        sidebarWidth,
        sidebarTopPercentage,
        bottomPaneSize,
      }),
    }
  )
);
