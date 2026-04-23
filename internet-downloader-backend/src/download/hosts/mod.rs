use std::collections::HashMap;

use reqwest::Client;
use rquickjs::{Class, Ctx, FromJs, JsLifetime, prelude::This};
use serde::Deserialize;
use tracing::debug;

#[derive(Debug)]
pub struct DownloadTask {
    pub url: String,
    pub task_type: TaskType,
}

impl DownloadTask {
    pub fn new(url: String, task_type: TaskType) -> Self {
        Self {
            url,
            task_type,
        }
    }
}

impl FromJs<'_> for DownloadTask {
    fn from_js(_ctx: &rquickjs::Ctx<'_>, value: rquickjs::Value<'_>) -> rquickjs::Result<Self> {
        let object = value.as_object().ok_or(rquickjs::Error::new_from_js(value.type_name(), "Object"))?;

        let url = object.get("url")?;
        let task_type = object.get("task_type")?;

        Ok(Self {
            url,
            task_type,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TaskType {
    File(FileTask),
    Folder(FolderTask),
}

impl<'js> FromJs<'js> for TaskType {
    fn from_js(ctx: &rquickjs::Ctx<'js>, value: rquickjs::Value<'js>) -> rquickjs::Result<Self> {
        let object = value.as_object().ok_or(rquickjs::Error::new_from_js(value.type_name(), "Object"))?;

        let type_str: String = object.get("type").unwrap_or_else(|_| "file".to_string());

        match type_str.as_str() {
            "file" => {
                Ok(Self::File(FileTask::from_js(ctx, value)?))
            }
            "folder" => {
                Ok(Self::Folder(FolderTask::from_js(ctx, value)?))
            }
            _ => Err(rquickjs::Error::new_from_js("type", "Unknown TaskType")),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct FileTask {
    pub url: String,
    pub file_name: String,
}

impl FileTask {
    pub fn new(url: impl Into<String>, file_name: String) -> Self {
        Self { 
            url: url.into(),
            file_name,
        }
    }

    pub const fn file_name(&self) -> &String {
        &self.file_name
    }
}

impl FromJs<'_> for FileTask {
    fn from_js(_ctx: &rquickjs::Ctx<'_>, value: rquickjs::Value<'_>) -> rquickjs::Result<Self> {
        let obj = value.as_object().ok_or(rquickjs::Error::new_from_js(value.type_name(), "Object"))?;
        
        Ok(FileTask {
            url: obj.get("url")?,
            file_name: obj.get("file_name")?, 
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct FolderTask {
    pub folder_name: String,
    pub files: Vec<TaskType>
}

impl FromJs<'_> for FolderTask {
    fn from_js(_ctx: &rquickjs::Ctx<'_>, value: rquickjs::Value<'_>) -> rquickjs::Result<Self> {
        let obj = value.as_object().ok_or(rquickjs::Error::new_from_js(value.type_name(), "Object"))?;

        Ok(Self {
            folder_name: obj.get("folder_name")?,
            files: obj.get("files")?,
        })
    }
}

impl FolderTask {
    pub fn new(folder_name: String, files: Vec<TaskType>) -> Self {
        Self { folder_name, files }
    }

    pub const fn folder_name(&self) -> &String {
        &self.folder_name
    }
}

#[derive(rquickjs::class::Trace, JsLifetime)]
#[rquickjs::class]
pub struct Request {
    url: String,
    body: Option<String>,
    headers: HashMap<String, String>,
    method: String,
}

impl Request {
    pub fn new(url: String) -> Self {
        Self {
            url,
            body: None,
            headers: HashMap::new(),
            method: "get".to_string(),
        }
    }
}

#[rquickjs::methods]
impl Request {
    pub fn body<'js>(This(this): This<Class<'js, Self>>, body: String) -> Class<'js, Self> {
        this.borrow_mut().body = Some(body);
        this
    }

    pub fn header<'js>(This(this): This<Class<'js, Self>>, key: String, value: String) -> Class<'js, Self> {
        this.borrow_mut().headers.insert(key, value);
        this
    }

    pub fn method<'js>(This(this): This<Class<'js, Self>>, method: String) -> Class<'js, Self> {
        this.borrow_mut().method = method;
        this
    }

    pub async fn send(This(this): This<Class<'_, Self>>) -> String {
        let client = Client::new();

        let method = this.borrow().method.to_lowercase();
        let url = &this.borrow().url;
        let headers = &this.borrow().headers;
        let body = &this.borrow().body;

        let mut request_builder = if method == "get" {
            client.get(url)
        } else if method == "post" { 
            client.post(url)
        } else {
            todo!()
        };

        for (key, value) in headers {
            request_builder = request_builder.header(key, value);
        }

        if let Some(body) = body {
            request_builder = request_builder.body(body.to_string());
        }

        // Return response
        request_builder.send().await.unwrap().text().await.unwrap()
    }
}

#[derive(rquickjs::class::Trace, JsLifetime)]
#[rquickjs::class]
pub struct Utils {

}

#[rquickjs::methods]
impl Utils {
    #[qjs(rename = "fetch")]
    pub async fn fetch(&self, url: String) -> String {
        debug!("Rust is fetching: {}", url);
        let client = Client::new();

        let fetched = client.get(url).send().await.unwrap().text().await.unwrap();
        debug!("sending: {}", fetched);
        fetched
        //"<html><body><a id='download' href='real_link.zip'>...</body></html>".to_string()
    }

    pub fn request<'js>(&self, ctx: Ctx<'js>, url: String) -> rquickjs::Result<Class<'js, Request>>  {
        Class::instance(ctx, Request::new(url))
    }
    
    pub fn log(&self, msg: String) {
        debug!("[PLUGIN] {}", msg);
    }
}