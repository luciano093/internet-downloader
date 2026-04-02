use reqwest::Client;
use tokio::sync::mpsc::UnboundedSender;

use crate::client_state_manager::UiStateEvent;
use crate::download::ManagerCommand;
use crate::network_manager::NetworkConfig;
use crate::plugin_registry::PluginRegistryHandler;
use crate::state_manager::StateManager;

#[derive(Clone)]
pub struct AppContext {
    pub client: Client,
    pub network_config: NetworkConfig,
    pub download_manager: UnboundedSender<ManagerCommand>,
    pub ui_sender: UnboundedSender<UiStateEvent>,
    pub db_manager: StateManager,
    pub plugin_registry: PluginRegistryHandler,
}