use internet_downloader_backend::state_manager::StateManager;
use internet_downloader_backend::download::DownloadManager;

use tokio::{fs::File};

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

    loop {

    }
}
