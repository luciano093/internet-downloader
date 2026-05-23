use reqwest::Client;
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::client_state_manager::UiStateEvent;
use crate::app_manager::AppManagerCommand;
use crate::download_writer_manager::DownloadWriterManager;
use crate::network_manager::NetworkConfig;
use crate::plugin_registry::PluginRegistryHandler;
use crate::db::state_manager::StateManager;

#[derive(Clone)]
pub struct AppContext {
    pub client: Client,
    pub network_config: NetworkConfig,
    pub app_manager: mpsc::Sender<AppManagerCommand>,
    pub ui_sender: UnboundedSender<UiStateEvent>,
    pub db_manager: StateManager,
    pub plugin_registry: PluginRegistryHandler,
    pub writer_handle: DownloadWriterManager,
}
