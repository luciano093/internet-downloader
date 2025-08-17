export default function DownloadTask({ text }: { text: string }) {
    return <>
        <div className="relative flex items-center bg-gray-600 hover:bg-gray-400 hover:text-gray-800 hover:cursor-default py-[2px] px-2">
            <div className="pr-2">
                <img src="/public/vite.svg" className="h-5 w-5 object-contain" />
            </div>
            <div>
                {text}
            </div>
        </div>
    </>
}