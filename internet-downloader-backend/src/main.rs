use std::convert::Infallible;
use std::time::Duration;
use std::{process::exit, sync::Arc};


use axum::Json;
use axum::extract::Path;
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Sse};
use axum::http::StatusCode;
use axum::routing::{delete, get, put};
use internet_downloader_backend::state_manager::StateManager;
use internet_downloader_backend::download::{DownloadId, DownloadManager};


use reqwest::Method;
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tokio::{fs::File, signal, sync::Mutex};
use axum::{extract::State, routing::post, Router};
use tower_http::cors::{self, Any, CorsLayer};
use tracing::{debug, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[tokio::main]
async fn main() {
    let _ = std::fs::remove_file("debug.log");
    let file_appender = tracing_appender::rolling::never(".", "debug.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .pretty()
        .with_target(false)
        .with_filter(EnvFilter::new("internet_downloader_backend=trace"));

    let console_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_target(false)
        .with_filter(EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .init();

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
        .nest("/downloads", Router::new()
            .route("/", get(download_stream).post(add_download))
            .nest("/{download_id}", Router::new()
                .route("/", delete(delete_download))
                .route("/pause", post(pause_download))
                .route("/resume", post(resume_download))
                .route("/limit", put(limit_download))
                .nest("/files/{file_id}", Router::new()
                    .route("/limit", put(limit_file))
                )
            )
        )
        .nest("/hosts/{host_name}", Router::new()
            .route("/limit", put(limit_host))
        )
        .route("/limit", put(limit_network))
        .with_state(download_manager)
        .layer(cors);

    let listener = tokio::net::TcpListener::bind("localhost:3211").await.unwrap();

    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();

        info!("Exiting program.");

        exit(0);
    });

    let addr = listener.local_addr().unwrap();
    info!("Server started at localhost:{}", addr.port());

    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize, Debug)]
struct DownloadSettings {
    url: String,
}

#[axum::debug_handler] 
async fn add_download(State(manager): State<Arc<Mutex<DownloadManager>>>, Json(json): Json<DownloadSettings>) -> impl IntoResponse {
    debug!(url = %json.url, "Received download query");

    match manager.lock().await.queue_download(json.url).await {
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
                            yield Ok(Event::default().event("delta").data(data));
                        }
                        Some(Err(err)) => {
                            yield Ok(Event::default().event("error").data(format!("Error: {}", err)));
                        }
                        None => break,
                    }
                }
                _ = snapshot_interval.tick() => {
                    let snapshot = manager.lock().await.get_snapshot().await;

                    let snapshot_json = serde_json::to_string(&snapshot).unwrap();

                    yield Ok(Event::default().event("snapshot").data(snapshot_json));
                }
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Deserialize, Debug)]
struct DownloadPath {
    download_id: DownloadId,
}

/// By default deletes a download from the database. `from_disk` signals to delete the actual file from the disk too 
#[derive(Deserialize, Debug)]
struct DeleteDownloadSettings {
    from_disk: Option<bool>,
}

#[axum::debug_handler] 
async fn delete_download(State(manager): State<Arc<Mutex<DownloadManager>>>, Path(path): Path<DownloadPath>, Json(json): Json<DeleteDownloadSettings>) -> impl IntoResponse {
    debug!(url = %path.download_id, from_disk = json.from_disk.unwrap_or(false), "Received download deletion query");

    manager.lock().await.remove_download(path.download_id, json.from_disk.unwrap_or(false)).await;
}

#[axum::debug_handler] 
async fn pause_download(State(manager): State<Arc<Mutex<DownloadManager>>>, Path(path): Path<DownloadPath>) -> impl IntoResponse {
    debug!(download_id = %path.download_id, "Received download pause query");

    manager.lock().await.pause_download(path.download_id).await;
}

#[axum::debug_handler] 
async fn resume_download(State(manager): State<Arc<Mutex<DownloadManager>>>, Path(path): Path<DownloadPath>) -> impl IntoResponse {
    debug!(download_id = %path.download_id, "Received download pause query");

    manager.lock().await.resume_download(path.download_id).await;
}

#[derive(Deserialize, Debug)]
struct LimitNetworkSettings {
    bandwidth_limit: Option<u64>,
}

#[axum::debug_handler] 
async fn limit_network(State(manager): State<Arc<Mutex<DownloadManager>>>, Json(json): Json<LimitNetworkSettings>) -> impl IntoResponse {
    debug!(bandwidth_limit = ?json.bandwidth_limit, "Received network limit");

    manager.lock().await.set_global_limit(json.bandwidth_limit);
}

#[derive(Deserialize, Debug)]
struct LimitHostSettings {
    host: String,
    bandwidth_limit: Option<u64>,
}

#[axum::debug_handler] 
async fn limit_host(State(manager): State<Arc<Mutex<DownloadManager>>>, Json(json): Json<LimitHostSettings>) -> impl IntoResponse {
    debug!(bandwidth_limit = ?json.bandwidth_limit, host = json.host, "Received network limit");

    manager.lock().await.set_host_limit(json.host, json.bandwidth_limit);
}

#[derive(Deserialize, Debug)]
struct LimitDownloadSettings {
    bandwidth_limit: Option<u64>,
}

#[axum::debug_handler] 
async fn limit_download(State(manager): State<Arc<Mutex<DownloadManager>>>, Path(path): Path<DownloadPath>, Json(json): Json<LimitDownloadSettings>) -> impl IntoResponse {
    debug!(bandwidth_limit = ?json.bandwidth_limit, download_id = *path.download_id, "Received network limit");

    manager.lock().await.set_download_limit(path.download_id, json.bandwidth_limit);
}

#[derive(Deserialize, Debug)]
struct FilePath {
    download_id: DownloadId,
    file_id: usize,
}

#[derive(Deserialize, Debug)]
struct LimitFileSettings {
    bandwidth_limit: Option<u64>,
}

#[axum::debug_handler] 
async fn limit_file(State(manager): State<Arc<Mutex<DownloadManager>>>, Path(path): Path<FilePath>, Json(json): Json<LimitFileSettings>) -> impl IntoResponse {
    debug!(bandwidth_limit = ?json.bandwidth_limit, download = *path.download_id, file_id = path.file_id, "Received network limit");

    manager.lock().await.set_file_limit(path.download_id, path.file_id, json.bandwidth_limit);
}