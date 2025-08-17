use std::convert::Infallible;
use std::{process::exit, sync::Arc};


use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Sse};
use axum::http::StatusCode;
use axum::routing::get;
use internet_downloader_backend::{download::DownloadManagerError, state_manager::StateManager};
use internet_downloader_backend::download::DownloadManager;


use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tokio::{fs::File, signal, sync::Mutex};
use axum::{extract::{Query, State}, routing::post, Router};
use tower_http::cors::{self, CorsLayer};

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

    let cors = CorsLayer::new()
        .allow_origin(cors::Any);

    let app = Router::new()
        .route("/add-download", post(add_download))
        .route("/downloads", get(download_stream))
        .with_state(download_manager)
        .layer(cors);

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
async fn add_download(State(manager): State<Arc<Mutex<DownloadManager>>>, Query(params): Query<DownloadQuery>) -> impl IntoResponse {
    println!("received: {:?}", params);

    match manager.lock().await.add_download(&params.url).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(DownloadManagerError::Parse(err)) => {
            (StatusCode::BAD_REQUEST, err.to_string()).into_response()
        },
    }
}

async fn download_stream(State(manager): State<Arc<Mutex<DownloadManager>>>) -> impl IntoResponse  {
    let receiver = manager.lock().await.download_subscribe();

    let stream = BroadcastStream::new(receiver).map(|result| -> Result<Event, Infallible> {
        match result {
            Ok(update) => {
                let data = serde_json::to_string(&update).unwrap_or_else(|_| "error".to_string());
                Ok(Event::default().data(data))
            }
            Err(err) => Ok(Event::default().data(format!("Data stream error: {}", err)))
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}