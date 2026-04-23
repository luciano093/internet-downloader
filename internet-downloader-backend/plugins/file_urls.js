// @ts-check
/// <reference path="./host.d.ts" />

/** @type {import("./host").Plugin} */
export default {
    supports_regex: ["\.[a-z0-9]{2,5}(?:[?#].*)?$"],

    async parse(url, utils) {

        /** @type {string} */
        let file_name = getFileName(url);
        
        return {
            url: url,
            task_type: {
                type: "file",
                file_name: file_name,
                url: url,
            }
        };
    }
}

/**
 * @param {string} urlStr
 */
function getFileName(urlStr) {
        let path = urlStr.split('?')[0].split('#')[0];

    // Remove trailing slashes (e.g., "site.com/dir/" -> "site.com/dir")
    if (path.endsWith('/')) {
        path = path.slice(0, -1);
    }
    const parts = path.split('/');
    const fileName = parts[parts.length - 1];

    return fileName || "file"; 
}