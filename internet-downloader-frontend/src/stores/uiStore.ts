import { create } from 'zustand'

export type ModalType = 'add' | 'remove' | null;

interface UiState {
  activeModal: ModalType;
  openModal: (modal: ModalType) => void;
  closeModal: () => void;
}

export const useUiStore = create<UiState>((set) => ({
  activeModal: null,
  openModal: (modal) => set({ activeModal: modal }),
  closeModal: () => set({ activeModal: null }),
}));