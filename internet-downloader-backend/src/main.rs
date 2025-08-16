use std::{process::exit, sync::Arc};

use axum::{extract::{Query, State}, routing::post, Router};
use internet_downloader_backend::state_manager::StateManager;
use internet_downloader_backend::download::DownloadManager;

use reqwest::StatusCode;
use serde::Deserialize;
use tokio::{fs::File, signal, sync::Mutex};

#[tokio::main]
async fn main() {
    let db_file= File::open("mydb.sqlite3").await;
    if db_file.is_err() {
        File::create_new("mydb.sqlite3").await.unwrap();
    }

    let state_manager = StateManager::new("mydb.sqlite3").await.unwrap();
    state_manager.create_tables().await.unwrap();

    let mut download_manager = DownloadManager::new(state_manager);

    download_manager.load_state().await;

    download_manager.start_processing().await;

    let download_manager = Arc::new(Mutex::new(download_manager));

    let app = Router::new()
        .route("/add-download", post(add_download))
        .with_state(download_manager);

    let listener = tokio::net::TcpListener::bind("localhost:3211").await.unwrap();

    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();

        println!("Exiting program.");

        exit(0);
    });

    let addr = listener.local_addr().unwrap();
    println!("Server started at localhost:{}", addr.port());

    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize, Debug)]
struct DownloadQuery {
    url: String,
}

#[axum::debug_handler] 
async fn add_download(State(manager): State<Arc<Mutex<DownloadManager>>>, Query(params): Query<DownloadQuery>) -> StatusCode {
    println!("received: {:?}", params);

    manager.lock().await.add_download(&params.url).await.unwrap();
    
    StatusCode::OK
}
