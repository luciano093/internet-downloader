import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { useDownloadStore } from "@/stores/downloadStore";
import { useUiStore } from "@/stores/uiStore";
import { useMutation } from "@tanstack/react-query";
import { Checkbox } from "./ui/checkbox";
import { Field, FieldContent, FieldLabel } from "./ui/field";
import { useState } from "react";

export function RemoveDownloadModal() {
    const activeModal = useUiStore((state) => state.activeModal);
    const closeModal = useUiStore((state) => state.closeModal);
    const [deleteChecked, setDeleteChecked] = useState(false);
    
    const selectedId = useDownloadStore((state) => state.selectedId);
    const setSelectedId = useDownloadStore((state) => state.setSelectedId);

    const isOpen = activeModal === 'remove';

    const removeMutation = useMutation({
        mutationFn: async ({ id, from_disk }: { id: number; from_disk: boolean }) => {
            return fetch(`http://localhost:3211/downloads/${id}`, {
                method: "DELETE",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    from_disk: from_disk
                }),
            });
        },
        onSuccess: (_, variables) => {
            if (selectedId === variables.id) {
                setSelectedId(null); // Clear the selection!
            }
        }
    });

    const handleRemove = () => {
        if (selectedId === null) return;

        removeMutation.mutate({ 
            id: selectedId, 
            from_disk: deleteChecked 
        });
        closeModal();
    };

    return (
        <Dialog open={isOpen} onOpenChange={(open) => !open && closeModal()}>
            <DialogContent className="bg-background text-foreground rounded-sm border-border sm:max-w-md">
                <DialogHeader>
                    <DialogTitle>Remove Download</DialogTitle>
                </DialogHeader>
                
                <div className="py-4 text-sm">
                    Are you sure you want to remove this download?

                    <Field orientation="horizontal" className="items-center mt-5">
                        <Checkbox
                            checked={deleteChecked}
                            onCheckedChange={(checked) => setDeleteChecked(checked === true)} 
                            id="delete-file-from-disk-checkbox"
                            name="delete-file-from-disk-checkbox"
                        />
                        <FieldContent>
                            <FieldLabel htmlFor="delete-file-from-disk-checkbox" className="font-normal">
                                Delete from disk?
                            </FieldLabel>
                        </FieldContent>
                    </Field>
                </div>

                <DialogFooter className="bg-background">
                    <button 
                        onClick={handleRemove}
                        className="h-8 px-4 rounded-sm bg-accent text-[13px] text-foreground hover:bg-accent-foreground/15 transition-colors cursor-pointer"
                    > Remove </button>
                    <button 
                        onClick={() => closeModal()}
                        className="h-8 px-4 rounded-sm bg-accent text-[13px] text-foreground hover:bg-accent-foreground/15 transition-colors cursor-pointer"
                    > Cancel </button>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    );
}