import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { useUiStore } from "@/stores/uiStore";
import { useState } from "react";

export function AddDownloadModal() {
    const activeModal = useUiStore((state) => state.activeModal);
    const closeModal = useUiStore((state) => state.closeModal);

    const isAddModalOpen = activeModal === 'add';

    const [urls, setUrls] = useState("");
    const[savePath, setSavePath] = useState("/downloads/completed/");
    const [startNow, setStartNow] = useState(true);

    const handleDownload = () => {
        const linkArray = urls.split('\n').filter(link => link.trim() !== '');
        
        const payload = { urls: linkArray, savePath, startNow };
        console.log("Ready to send to API:", payload);

        for (let url of linkArray) {
            fetch(`http://localhost:3211/downloads`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({
                    url: url
                }),
            });
        }
        
        closeModal();
    };

    return (
        <Dialog 
            open={isAddModalOpen} 
            onOpenChange={(open) => {
                if (!open) closeModal();
            }}
        >
            <DialogContent className="bg-background text-foreground rounded-sm border-border w-fit sm:max-w-[90vw] min-w-[500px]">
                <DialogHeader>
                    <DialogTitle className="text-foreground">Add download links</DialogTitle>
                </DialogHeader>
                
                <div className="flex flex-col gap-4 py-4">
                
                {/* Links Input */}
                <div className="flex flex-col gap-1.5">
                    <label className="text-xs font-medium text-foreground">Download URLs</label>
                    <textarea
                    autoFocus
                    value={urls}
                    onChange={(e) => setUrls(e.target.value)}
                    placeholder="http://..."
                    className="min-h-[120px] w-full rounded-sm bg-[#1A1C1E] border border-border min-w-[500px] p-2.5 text-[13px] text-foreground placeholder:text-gray-600 focus:border-gray-500 focus:outline-none resize"
                    />
                </div>

                {/* Save Location */}
                <div className="flex flex-col gap-1.5">
                    <label className="text-xs font-medium text-gray-300">Save Path</label>
                    <div className="flex items-center gap-2">
                    <input
                        type="text"
                        value={savePath}
                        onChange={(e) => setSavePath(e.target.value)}
                        defaultValue="/downloads/completed/"
                        className="h-8 w-full rounded-sm bg-[#1A1C1E] border border-border px-2.5 text-[13px] text-foreground focus:border-gray-500 focus:outline-none"
                    />
                    </div>
                </div>    

                {/* Additional Options */}
                <div className="flex items-center gap-2 mt-2">
                    <input 
                    type="checkbox" 
                    id="start-now" 
                    defaultChecked 
                    onChange={(e) => setStartNow(e.target.checked)}
                    className="h-3.5 w-3.5 rounded-sm cursor-pointer" 
                    />
                    <label htmlFor="start-now" className="text-[13px] text-foreground cursor-pointer select-none">
                    Start download immediately
                    </label>
                </div>

                </div>

                {/* Footer Buttons */}
                <DialogFooter className="mt-2 bg-background w-full mx-0">
                    <div className="justify-center flex w-full gap-2">
                        <button 
                            onClick={handleDownload}
                            className="h-8 px-4 rounded-sm bg-accent text-[13px] text-foreground hover:bg-accent-foreground/15 transition-colors cursor-pointer"
                        >
                            Download
                        </button>
                        <button 
                            onClick={() => closeModal()}
                            className="h-8 px-4 rounded-sm bg-accent text-[13px] text-foreground hover:bg-accent-foreground/15 transition-colors cursor-pointer"
                        >
                            Cancel
                        </button>
                    </div>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    );
}