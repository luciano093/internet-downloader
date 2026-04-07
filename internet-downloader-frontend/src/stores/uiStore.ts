import { create } from 'zustand'

interface UiState {
  isAddModalOpen: boolean
  setAddModalOpen: (isOpen: boolean) => void
}

export const useUiStore = create<UiState>((set) => ({
  isAddModalOpen: false,
  setAddModalOpen: (isOpen) => set({ isAddModalOpen: isOpen }),
}))