import type { FormEvent } from "react";
import DefaultButton from "./DefaultButton";

export default function DownloadUrlBar() {
    const OnSubmit = (event: FormEvent<HTMLFormElement>) => {
        event.preventDefault();

        const url = (new FormData(event.currentTarget)).get("url") ?? "";
        console.log("test: ", url);

        fetch(`http://localhost:3211/add-download?url=${url}`, {
            method: "POST",
            })
    };

    return <>
        <form onSubmit={OnSubmit} className="relative flex items-center bg-gray-600 w-full h-[40px]">
            <input className="bg-gray-800 border-2 border-gray-500 focus:outline-none w-full h-[30px] mx-[10px] px-3"
                placeholder="Download link"
                type="text"
                name="url"
                />
            <DefaultButton type="submit" text="Add" />
        </form>
    </>
}