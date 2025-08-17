import type { ButtonHTMLAttributes } from "react";

export default function DefaultButton({ text, ...props }: { text: string } & ButtonHTMLAttributes<HTMLButtonElement>) {
    return <>
        <button className="bg-gray-800 hover:bg-gray-700 cursor-pointer border-2 border-gray-500 hover:border-gray-400 rounded-lg w-fit px-2 h-9 mr-4"
            {...props}
        > {text} </button>
    </>
}