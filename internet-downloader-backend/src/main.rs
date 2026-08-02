use std::collections::HashMap;
use std::convert::Infallible;
use std::time::Duration;
use std::process::exit;

use axum::Json;
use axum::extract::Path;
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Sse};
use axum::http::StatusCode;
use axum::routing::{delete, get, patch};
use internet_downloader_backend::app::manager::AppManagerHandle;
use internet_downloader_backend::app::snapshot::AppSnapshotHandler;
use internet_downloader_backend::client_state_manager::DownloadSnapshot;
use internet_downloader_backend::db::state_manager::StateManager;

use internet_downloader_backend::download::items::{DownloadId, FileId};
use reqwest::Method;
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tokio::{fs::File, signal};
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
        .with_filter(EnvFilter::new("internet_downloader_backend=debug"));

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

    let snapshot_manager = AppSnapshotHandler::spawn(state_manager.clone());
    let app_manager = AppManagerHandle::spawn(state_manager, snapshot_manager);

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS, Method::PUT])
        .allow_origin(cors::Any)
        .allow_headers(Any);

    let app = Router::new()
        .nest("/downloads", Router::new()
            .route("/", get(download_stream).post(add_download))
            .nest("/{download_id}", Router::new()
                .route("/", delete(delete_download))
                .route("/pause", post(pause_download))
                .route("/resume", post(resume_download))
                .route("/settings", get(download_settings))
                .route("/settings", patch(apply_download_settings))
                .nest("/files/{file_id}", Router::new()
                    .route("/settings", get(file_settings))
                    .route("/settings", patch(apply_file_settings))
                )
            )
        )
        .nest("/hosts/{host_name}", Router::new()
            .route("/settings", get(host_settings))
            .route("/settings", patch(apply_host_settings))
        )
        .route("/settings", get(app_settings))
        .route("/settings", patch(apply_app_settings))
        .with_state(app_manager)
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
async fn add_download(State(manager): State<AppManagerHandle>, Json(json): Json<DownloadSettings>) -> impl IntoResponse {
    debug!(url = %json.url, "Received download query");

    match manager.queue_download(json.url).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(_) => {
            StatusCode::BAD_REQUEST.into_response()
        },
    }
}

async fn download_stream(State(manager): State<AppManagerHandle>) -> impl IntoResponse  {
    let receiver = manager.subscribe();

    let stream = async_stream::stream! {
        let downloads = manager.get_snapshot().await;
    
        let snapshot: Vec<DownloadSnapshot> = downloads
            .into_iter()
            .map(|(_id, download_snapshot)| {
                download_snapshot
            })
            .collect();
        
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
                    let downloads = manager.get_snapshot().await;
                    let snapshot: Vec<DownloadSnapshot> = downloads
                        .into_iter()
                        .map(|(_id, download_snapshot)| download_snapshot)
                        .collect();

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
async fn delete_download(State(manager): State<AppManagerHandle>, Path(path): Path<DownloadPath>, Json(json): Json<DeleteDownloadSettings>) -> impl IntoResponse {
    debug!(url = %path.download_id, from_disk = json.from_disk.unwrap_or(false), "Received download deletion query");

    let _ = manager.remove_download(path.download_id, json.from_disk.unwrap_or(false)).await;
}

#[axum::debug_handler] 
async fn pause_download(State(manager): State<AppManagerHandle>, Path(path): Path<DownloadPath>) -> impl IntoResponse {
    debug!(download_id = %path.download_id, "Received download pause query");

    let _ = manager.pause_download(path.download_id).await;
}

#[axum::debug_handler] 
async fn resume_download(State(manager): State<AppManagerHandle>, Path(path): Path<DownloadPath>) -> impl IntoResponse {
    debug!(download_id = %path.download_id, "Received download pause query");

    let _ = manager.resume_download(path.download_id).await;
}

#[derive(Debug, Clone, Default, PartialEq)]
enum PatchValue<T> {
    #[default]
    Unchanged,
    Clear,
    Set(T),
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for PatchValue<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(match Option::<T>::deserialize(d)? {
            Some(v) => PatchValue::Set(v),
            None => PatchValue::Clear,
        })
    }
}

#[axum::debug_handler] 
async fn app_settings(State(manager): State<AppManagerHandle>) -> impl IntoResponse {
    debug!( "Received app settings GET request");

    Json(manager.get_settings().await)
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct AppSettingsPatch {
    global_speed_limit: PatchValue<u64>,
    host_settings: Option<HashMap<String, HostSettingsPatch>>,
    download_settings: Option<HashMap<DownloadId, DownloadSettingsPatch>>,
}

#[axum::debug_handler] 
async fn apply_app_settings(State(manager): State<AppManagerHandle>, Json(settings): Json<AppSettingsPatch>) -> impl IntoResponse {
    debug!( "Received app settings PATCH request");

    apply_app_patch(manager, settings).await
}

async fn apply_app_patch(manager: AppManagerHandle, settings: AppSettingsPatch) -> Result<(), (StatusCode, String)> {
    match settings.global_speed_limit {
        PatchValue::Unchanged => Ok(()),
        PatchValue::Clear => manager.set_global_limit(None).await.map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
        PatchValue::Set(speed_limit) => manager.set_global_limit(Some(speed_limit)).await.map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }?;
    
    if let Some(host_settings_map) = settings.host_settings {
        for (host, host_settings) in host_settings_map {
            apply_host_patch(manager.clone(), host, host_settings).await?;
        }
    }

    if let Some(download_settings_map) = settings.download_settings {
        for (download_id, download_settings) in download_settings_map {
            apply_download_patch(manager.clone(), download_id, download_settings).await?;
        }
    }

    Ok(())
}

#[derive(Deserialize, Default, Debug)]
struct HostSettingsPatch {
    speed_limit: PatchValue<u64>,
}

#[axum::debug_handler] 
async fn apply_host_settings(State(manager): State<AppManagerHandle>, Path(host): Path<String>, Json(settings): Json<HostSettingsPatch>) -> impl IntoResponse {
    debug!(speed_limit = ?settings.speed_limit, host, "Received network limit");

    apply_host_patch(manager, host, settings).await
}

async fn apply_host_patch(manager: AppManagerHandle, host: String, settings: HostSettingsPatch) -> Result<(), (StatusCode, String)> {
    match settings.speed_limit {
        PatchValue::Unchanged => Ok(()),
        PatchValue::Clear    => manager.set_host_limit(host, None).await.map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
        PatchValue::Set(value)   => manager.set_host_limit(host, Some(value)).await.map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }
}

#[axum::debug_handler] 
async fn host_settings(State(manager): State<AppManagerHandle>, Path(host): Path<String>) -> impl IntoResponse {
    debug!( "Received host settings GET request");

    match manager.get_settings().await.get_host_settings(&host) {
        Some(host_settings) => Json(host_settings).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Deserialize, Default, Debug)]
struct DownloadSettingsPatch {
    speed_limit: PatchValue<u64>,
    file_settings: Option<HashMap<FileId, FileSettingsPatch>>,
}

#[axum::debug_handler] 
async fn apply_download_settings(State(manager): State<AppManagerHandle>, Path(download_id): Path<DownloadId>, Json(settings): Json<DownloadSettingsPatch>) -> impl IntoResponse {
    debug!(speed_limit = ?settings.speed_limit, download_id = (*download_id) as usize, "Received download limit");

    apply_download_patch(manager, download_id, settings).await
}

async fn apply_download_patch(manager: AppManagerHandle, download_id: DownloadId, settings: DownloadSettingsPatch) -> Result<(), (StatusCode, String)> {
    match settings.speed_limit {
        PatchValue::Unchanged => Ok(()),
        PatchValue::Clear    => manager.set_download_limit(download_id, None).await.map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
        PatchValue::Set(value)   => manager.set_download_limit(download_id, Some(value)).await.map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }?;

    if let Some(file_settings_map) = settings.file_settings {
        for (file_id, file_settings) in file_settings_map {
            apply_file_patch(manager.clone(), download_id, file_id, file_settings).await?;
        }
    }

    Ok(())
}

#[axum::debug_handler] 
async fn download_settings(State(manager): State<AppManagerHandle>, Path(download_id): Path<DownloadId>) -> impl IntoResponse {
    debug!( "Received download settings GET request");

    match manager.get_settings().await.get_download_settings(download_id) {
        Some(download_settings) => Json(download_settings).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Deserialize, Default, Debug)]
struct FileSettingsPatch {
    speed_limit: PatchValue<u64>,
}

#[axum::debug_handler] 
async fn apply_file_settings(State(manager): State<AppManagerHandle>, Path((download_id, file_id)): Path<(DownloadId, FileId)>, Json(settings): Json<FileSettingsPatch>) -> impl IntoResponse {
    debug!(speed_limit = ?settings.speed_limit, download_id = (*download_id) as usize, "Received download limit");

    apply_file_patch(manager, download_id, file_id, settings).await
}

async fn apply_file_patch(manager: AppManagerHandle, download_id: DownloadId, file_id: FileId, settings: FileSettingsPatch) -> Result<(), (StatusCode, String)> {
    let result: Result<(), (StatusCode, String)> = match settings.speed_limit {
        PatchValue::Unchanged => Ok(()),
        PatchValue::Clear    => manager.set_file_limit(download_id, file_id, None).await.map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
        PatchValue::Set(value)   => manager.set_file_limit(download_id, file_id, Some(value)).await.map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    };
    
    result
}

#[axum::debug_handler] 
async fn file_settings(State(manager): State<AppManagerHandle>, Path((download_id, file_id)): Path<(DownloadId, FileId)>) -> impl IntoResponse {
    debug!( "Received file settings GET request");

    let app_settings = manager.get_settings().await;
    let file_settings = app_settings
        .get_download_settings(download_id)
        .and_then(|download_settings| {
            download_settings.get_file_settings(&file_id)
        });

    match file_settings {
        Some(file_settings) => Json(file_settings).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
