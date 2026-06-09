// @ts-check
/// <reference path="./host.d.ts" />

/** @type {import("./host").Plugin} */
export default {
    supports_regex: ["^https?://"],

    async parse(url, utils) {
        return {
            url: url,
            task_type: {
                type: "file",
                file_name: null,
                url: url,
            }
        };
    }
}
