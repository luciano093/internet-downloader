export interface DownloadTask {
        url: string;
        task_type: TaskType;
    }

export type TaskType = 
    | { 
        type: "file", 
        file_name: string,
        url: string;
    }
    | { type: "folder",
        folder_name: string, 
        files: TaskType[],
    };

export interface RequestBuilder {
    header(name: string, value: string): RequestBuilder;
    cookie(name: string, value: string): RequestBuilder;
    method(method: "GET" | "POST" | "HEAD"): RequestBuilder;
    body(data: string): RequestBuilder;
    send(): Promise<string>; 
}

export interface Utils {
    request(url: string): RequestBuilder;
    log(msg: string): void;
}

    
interface BasePlugin {
    parse(url: string, utils: Utils): Promise<DownloadTask>;
}

export type Plugin = BasePlugin & (
    | { supports: string[]; supports_regex?: string[] }
    | { supports_regex: string[]; supports?: string[] } 
);

  