use std::convert::Infallible;
use std::time::Duration;
use std::{process::exit, sync::Arc};


use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Sse};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use internet_downloader_backend::state_manager::StateManager;
use internet_downloader_backend::download::{DownloadId, DownloadManager};


use reqwest::Method;
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tokio::{fs::File, signal, sync::Mutex};
use axum::{extract::{Query, State}, routing::post, Router};
use tower_http::cors::{self, Any, CorsLayer};

#[tokio::main]
async fn main() {
    // let plugin = JsPlugin::new("./plugins/test.js").await;
    // plugin.parse("./plugins/test.js");

    rustls::crypto::ring::default_provider().install_default()
        .expect("Failed to install rustls crypto provider");

    let db_file= File::open("mydb.sqlite3").await;
    if db_file.is_err() {
        File::create_new("mydb.sqlite3").await.unwrap();
    }

    let state_manager = StateManager::new("mydb.sqlite3").await.unwrap();
    state_manager.create_tables().await.unwrap();

    let mut download_manager = DownloadManager::new(state_manager);

    download_manager.load_state().await;

    download_manager.verify_downloads().await;

    download_manager.start_processing().await;

    let download_manager = Arc::new(Mutex::new(download_manager));

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_origin(cors::Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/add-download", post(add_download))
        .route("/downloads", get(download_stream))
        .route("/delete-download", delete(delete_download))
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

    match manager.lock().await.queue_download(params.url).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(()) => {
            StatusCode::BAD_REQUEST.into_response()
        },
    }
}

async fn download_stream(State(manager): State<Arc<Mutex<DownloadManager>>>) -> impl IntoResponse  {
    let manager_guard = manager.lock().await;
    let receiver = manager_guard.download_subscribe();
    let snapshot = manager_guard.get_snapshot().await;

    drop(manager_guard);

    let stream   = async_stream::stream! {
        let snapshot_json = serde_json::to_string(&snapshot).unwrap();

        // explicit turbofish as Infallible can't be inferred automatically
        yield Ok::<_, Infallible>(Event::default().event("snapshot").data(snapshot_json).retry(Duration::from_millis(100)));

        let mut broadcast_stream = BroadcastStream::new(receiver);
        let mut snapshot_interval = tokio::time::interval(Duration::from_secs(5));
        snapshot_interval.tick().await; 

        loop {
            tokio::select! {
                result = broadcast_stream.next() => {
                    match result {
                        Some(Ok(update)) => {
                            let data = serde_json::to_string(&update).unwrap();
                            // println!("sending: {}", data);
                            yield Ok(Event::default().event("delta").data(data));
                        }
                        Some(Err(err)) => {
                            yield Ok(Event::default().event("error").data(format!("Error: {}", err)));
                        }
                        None => break,
                    }
                }
                _ = snapshot_interval.tick() => {
                    println!("sending snapshot");
                    let snapshot = manager.lock().await.get_snapshot().await;

                    let snapshot_json = serde_json::to_string(&snapshot).unwrap();

                    yield Ok(Event::default().event("snapshot").data(snapshot_json));
                }
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// By default deletes a download from the database. `from_disk` signals to delete the actual file from the disk too 
#[derive(Deserialize, Debug)]
struct DownloadDeletion {
    id: usize, // id of the download to delete
    from_disk: Option<bool>,
}


#[axum::debug_handler] 
async fn delete_download(State(manager): State<Arc<Mutex<DownloadManager>>>, Query(params): Query<DownloadDeletion>) -> impl IntoResponse {
    println!("received: {:?}", params);

    manager.lock().await.remove_download(DownloadId(params.id)).await;

    "test"
}