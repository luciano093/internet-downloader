use std::{collections::{HashMap, HashSet}, ops::Deref, sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}}, time::{SystemTime, UNIX_EPOCH}};

use rquickjs::{Array, AsyncContext, AsyncRuntime, Context, Function, Module, Object, Promise, Runtime, WriteOptions};
use tokio::{fs::read_dir, sync::{mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}, oneshot}};
use regex::{Regex, escape};
use tracing::{debug, error, info, trace, warn};
use url::{Host, ParseError, Url};
use tokio_util::sync::CancellationToken;

use crate::download::hosts::{DownloadTask, Utils};

struct ParseRequest {
    url: String,
    plugin_id: PluginId,
    bytecode: Option<Arc<Vec<u8>>>,
    load_guard: LoadGuard,
    cancel_token: CancellationToken,
}

enum WorkerMessage {
    Parse(ParseRequest),
}

pub enum PluginRegistryMessage {
    Parse(String, oneshot::Sender<Option<DownloadTask>>, CancellationToken),
}

#[derive(Debug, Hash, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PluginId(usize);

impl Deref for PluginId {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct Worker {
    context: AsyncContext,
    receiver: UnboundedReceiver<WorkerMessage>,
    last_start: Arc<AtomicU64>,
}

impl Worker {
    fn new(receiver: UnboundedReceiver<WorkerMessage>, context: AsyncContext, last_start: Arc<AtomicU64>, ) -> Self {
        Self {
            receiver,
            context,
            last_start
        }
    }

    async fn spawn(mut self) {  
        self.context.with(|ctx| {
            let global = ctx.globals();
            // Create the cache object one time
            let _ = global.set("__PLUGIN_MODULES__", Object::new(ctx.clone())?);
            Ok::<_, rquickjs::Error>(())
        }).await.unwrap();

        while let Some(message) = self.receiver.recv().await {
            match message {
                WorkerMessage::Parse(parse_request) => {
                    self.handle_parse(parse_request).await;
                },
            }
        }
    }

    async fn handle_parse(&self, request: ParseRequest) {
        let ParseRequest { url, plugin_id, bytecode, load_guard, .. } = request;

        self.context.with(move |context| {
            self.last_start.store(current_millis(), Ordering::Relaxed);
            let globals = context.globals();

            // "cache" is a JS Object acting as HashMap<PluginId, ModuleObject>
            let cache: Object = globals.get("__PLUGIN_MODULES__").unwrap();
            let id_key = (*plugin_id).to_string();

            // try to load module from cache
            let module: Object = if let Some(module) = cache.get(&id_key).unwrap_or(None) {
                module
            } else {
                let bytecode = bytecode.expect("Missing bytecode for cold load");
                let module = unsafe { Module::load(context.clone(), &bytecode).unwrap() };

                let (module, promise) = module.eval().unwrap();
                promise.finish::<()>().unwrap();
                
                
                // Get the Namespace Object. Contains all the functions for this plugin
                let namespace = module.namespace().unwrap();
                
                // Save to cache
                cache.set(&id_key, namespace.clone()).unwrap();
                namespace
            };

            let default_export: Object = module.get("default")
                    .expect("Plugin missing export default");

            
            let parse: Function = default_export.get("parse").unwrap();

            self.last_start.store(0, Ordering::Relaxed); 
            let promise: Promise = parse.call((url, Utils { })).unwrap();
            let future = promise.into_future::<DownloadTask>();

            let value = context.clone();

            context.spawn(async move {
                tokio::select! {
                    result = future => {
                        match result {
                            Ok(download_task) => {
                                debug!("got task!: {:#?}", download_task);
                                let _ = load_guard.send(Some(download_task));
                            }
                            Err(err) => {
                                if err.is_exception() {
                                    let exception = value.catch(); 
                                    
                                    // Print the main error message
                                    if let Some(msg) = exception.as_string() {
                                        warn!("JS Exception: {}", msg.to_string().unwrap_or_default());
                                    }

                                    // Print the stack trace
                                    if let Some(stack) = exception.as_object().and_then(|o| o.get::<_, String>("stack").ok()) {
                                        warn!("Stack Trace:\n{}", stack);
                                    }
                                } else {
                                    // Rust error
                                    error!("Rust error: {}", err);
                                }

                                let _ = load_guard.send(None);
                            }
                        }
                    }
                    _ = request.cancel_token.cancelled() => {
                        drop(load_guard);
                    }
                }

            });
        }).await;
    }
}

struct LoadGuard{
    active_load: Arc<AtomicUsize>,
    reply: Option<oneshot::Sender<Option<DownloadTask>>>,
}

impl LoadGuard {
    fn send(mut self, message: Option<DownloadTask>) {
        let _ = self.reply.take().unwrap().send(message);
    }
}

impl Drop for LoadGuard {
    fn drop(&mut self) {
        self.active_load.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

        if let Some(sender) = self.reply.take() {
            // If we are dropping and haven't sent a value, it implies failure/cancellation
            let _ = sender.send(None);
        }
    }
}

struct WorkerHandle {
    loaded: HashSet<PluginId>,
    sender: UnboundedSender<WorkerMessage>,
    _runtime: AsyncRuntime,
    active_load: Arc<AtomicUsize>, // number of functions the worker is processing
}

impl WorkerHandle {
    async fn new() -> Self {
        let (sender, receiver) = unbounded_channel();

        let runtime = AsyncRuntime::new().unwrap();
        let context = AsyncContext::full(&runtime).await.unwrap();

        let last_start = Arc::new(AtomicU64::new(0));
        let handler_start = last_start.clone();

        let runtime_clone = runtime.clone();

        tokio::spawn(async move {
            runtime_clone.drive().await; 
        });

        // If the provided closure returns true the interpreter will raise and uncatchable exception and return control flow to the caller.
        runtime.set_interrupt_handler({Some(Box::new(move || {
            let start = last_start.load(Ordering::Relaxed);

            if start == 0 {
                return false;
            }

            let now = current_millis();

                // If execution takes longer than 5s, terminate it
                if now.saturating_sub(start) > 5000 {
                    return true;
                }
                false
            }))
        }).await;

        let worker = Worker::new(receiver, context, handler_start);

        tokio::spawn(async {
            worker.spawn().await;
        });

        Self {
            loaded: HashSet::new(),
            sender,
            _runtime: runtime,
            active_load: Arc::new(0.into()),
        }
    }

    fn parse(&self, url: String, plugin_id: PluginId, bytecode: Option<Arc<Vec<u8>>>, reply: oneshot::Sender<Option<DownloadTask>>, cancel_token: CancellationToken) {
        let _ = self.sender.send(WorkerMessage::Parse(ParseRequest { url, plugin_id, bytecode, load_guard: LoadGuard { active_load: self.active_load.clone(), reply: Some(reply) }, cancel_token }));
    }
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn create_handles(amount: usize) -> Vec<WorkerHandle> {
    let mut handles = Vec::new();

    for _ in 0..amount {
        handles.push(WorkerHandle::new().await);
    }

    handles
}

struct RegistryEntry {
    plugin_path: String,
    plugin_name: String,
    bytecode: Arc<Vec<u8>>,
    supports: Vec<(Regex, usize)>, // regex and a score based in length to resolve ties
    excludes: Vec<Regex>,
    priority: i32,
}

impl RegistryEntry {
    fn get_match_metric(&self, url: &str) -> Option<(i32, usize, String)> {
        // excludes any urls are are matched by the excludes regexes
        if self.excludes.iter().any(|regex| regex.is_match(url)) {
            return None; 
        }

        // we get all regexes that match our url, if there are multiple, we get the one with a highest score (matches more of the url)
        let best_specificity = self.supports.iter()
            .filter(|(regex, _)| {
                regex.is_match(url)
            })
            .map(|(_, score)| *score)
            .max()?; 
        
        Some((self.priority, best_specificity, self.plugin_name.clone()))
    }
}

struct PluginRegistry {
    receiver: UnboundedReceiver<PluginRegistryMessage>,
    entries: Vec<RegistryEntry>,
    runtimes: Vec<WorkerHandle>,
    host_cache: HashMap<Host, Vec<PluginId>>, // cache for plugins that only contain simple domains
    complex_plugins: Vec<PluginId>, // list of plugins that contain regexes that must be always checked
}

impl PluginRegistry {
    async fn new(receiver: UnboundedReceiver<PluginRegistryMessage>) -> Self {
        let (entries, host_cache, complex_plugins) = load_plugins("./plugins").await;

        let runtimes = create_handles(6).await;

        // create plugin channels and add them to the hashmap along with their id

        Self {
            receiver,
            entries,
            runtimes,
            host_cache,
            complex_plugins,
        }
    }

    async fn run(mut self) {
        while let Some(message) = self.receiver.recv().await {
            match message {
                PluginRegistryMessage::Parse(url, sender, cancel_token) => {
                    let mut candidates = HashSet::new(); 

                    for id in &self.complex_plugins {
                        candidates.insert(*id);
                    }

                    debug!("host name: {:#?}", Url::parse(&url).unwrap().host());

                    if let Some(host) = Url::parse(&url).unwrap().host() {
                        if let Some(ids) = self.host_cache.get(&host.to_owned()) {
                            for id in ids {
                                candidates.insert(*id);
                            }
                        }
                    }

                    let candidates: Vec<PluginId> = candidates.into_iter().collect();
                    for candiate in &candidates {
                        debug!("found candidate: {}", self.entries[**candiate].plugin_name);
                    }

                    if let Some(plugin_id) = self.find_best_plugin(&url, &candidates) {
                        debug!("found best plugin: {}", self.entries[*plugin_id].plugin_name);
                        let worker_index = self.select_best_worker(plugin_id);
                        let worker = &mut self.runtimes[worker_index];

                        let contains_plugin = worker.loaded.contains(&plugin_id);

                        let bytecode = if !contains_plugin {
                            worker.loaded.insert(plugin_id);
                            Some(self.entries[plugin_id.0].bytecode.clone())
                        } else {
                            None
                        };

                        worker.active_load.fetch_add(1, Ordering::Relaxed);

                        worker.parse(url, plugin_id, bytecode, sender, cancel_token);
                    } else { 
                        warn!("didn't find plugin");
                        let _ = sender.send(None);
                    }
                },
            }
        }
    }

    fn find_best_plugin(&self, url: &str, candidates: &[PluginId]) -> Option<PluginId> {

        candidates.iter()
        .filter_map(|&plugin_id| {
            let entry = &self.entries[*plugin_id];

            let entry = entry.get_match_metric(url).map(|(priority, specificity, name)| {
                (plugin_id, priority, specificity, name)
            });

            entry
        })
        .max_by(|(_, specificity_a, priority_a, name_a), (_, specificity_b, priority_b, name_b)| {
            // First we check specificity, for example in "https://youtube.com/watch/?v=", 
            // a plugin that matches only "https://youtube.com" will lose against a plugin that matches "https://youtube.com/watch"
            specificity_a.cmp(specificity_b)
        
            //then we match by priority to deal with ties, if two plugins match the exact same string, 
            // then the one with a higher priority wins
            .then(priority_a.cmp(priority_b))
            
            // if both priority and specificity are the same, we just return the plugin with the name
            // that comes first alphabetically
            .then(name_b.cmp(name_a)) 
        })
        .map(|(id, _, _, _)| id)
    }

    fn select_best_worker(&self, plugin_id: PluginId) -> usize {
        let mut best_index = 0;
        // Start with the worst possible score, and with the worker not containing the plugin
        let mut best_score = (usize::MAX, false); 

        for (idx, handle) in self.runtimes.iter().enumerate() {
            let load = handle.active_load.load(Ordering::Relaxed);
            let contains_plugin = handle.loaded.contains(&plugin_id);
            
            // 0 load, contains (false) < 0 load, contains (true).
            // We prioritize workers with less active load
            // if workers have the same active load we prioritize the worker that
            // already contains the plugin. Because false is less than true, and we want 
            // the one with minor load, we want that if contains_plugin is true, to change to false to be less.
            
            let score = (load, !contains_plugin);

            if score < best_score {
                best_score = score;
                best_index = idx;
            }
        }
        
        best_index
    }
}

#[derive(Debug, Clone)]
pub struct PluginRegistryHandler {
    sender: UnboundedSender<PluginRegistryMessage>
}

impl PluginRegistryHandler {
    pub async fn spawn() -> Self {
        let (sender, receiver) = unbounded_channel();

        let plugin_registry = PluginRegistry::new(receiver).await;

        tokio::spawn(async move {
            plugin_registry.run().await;
        });

        PluginRegistryHandler { sender }
    }

    pub fn parse(&self, url: String, sender: oneshot::Sender<Option<DownloadTask>>, cancel_token: CancellationToken) {
        let _ = self.sender.send(PluginRegistryMessage::Parse(url, sender, cancel_token));
    }
}

async fn load_plugins(plugins_path: &str) -> (Vec<RegistryEntry>, HashMap<Host, Vec<PluginId>>, Vec<PluginId>) {
    let mut plugin_folder = read_dir(plugins_path).await.unwrap();

    let runtime = Runtime::new().unwrap();
    let context = Context::full(&runtime).unwrap();

    let mut entries = Vec::new();
    let mut host_map: HashMap<Host, Vec<PluginId>> = HashMap::new();
    let mut complex_plugins = Vec::new();
    let mut id = 0usize;

    while let Ok(Some(path)) = plugin_folder.next_entry().await {
        let name = path.file_name().to_str().unwrap().to_string();
        let path = path.path();

        trace!("Found {}", name);

        // Skip non-js paths
        if path.extension().map_or(false, |extension| extension != "js") {
            continue;
        }

        let source_code = match tokio::fs::read_to_string(&path).await {
            Ok(s) => s,
            Err(_) => continue, // Skip unreadable files
        };

        let absolute_path = tokio::fs::canonicalize(&path).await.unwrap();
        let module_name = absolute_path.to_string_lossy().replace("\\", "/");

        let result = context.with(|context| -> rquickjs::Result<(Vec<(Regex, usize)>, Vec<Regex>, i32, Vec<u8>)> {
            
            // Compile module
            let module = Module::declare(context.clone(), module_name, source_code).unwrap();

            let bytecode: Vec<u8> = module.write(WriteOptions::default()).unwrap();

            let (module, promise): (_, Promise) = module.eval().unwrap();
            promise.finish::<()>().unwrap();

            let plugin: Object = module.get("default").expect("Plugin is missing 'export default { ... }'");
            let supports_arr: Option<Array> = plugin.get("supports").unwrap();
            let supports_regex: Option<Array> = plugin.get("supports_regex").unwrap();

            if supports_arr.is_none() && supports_regex.is_none() {
                panic!("Plugin object should have either a 'supports' list or a 'supports_regex' list");
            }

            let priority: i32 = plugin.get("priority").unwrap_or(0);

            let mut supports = Vec::new();
            let mut excludes = Vec::new();

            let is_complex = supports_regex.is_some() && !supports_regex.as_ref().unwrap().is_empty();

            if let Some(supports_regex) = supports_regex {
                for item in supports_regex.iter::<String>() {
                    let string = item?;

                    if let Ok(regex) = Regex::new(&string) {
                        supports.push((regex, string.len()));
                    } else {
                        warn!("Plugin {} had a wrong regex: {}", name, string);
                    }
                }
            }
            
            if let Some(supports_arr) = supports_arr {
                for item in supports_arr.iter::<String>() {
                    let string = item?;

                    if let Some(str) = string.strip_prefix('!') {
                        excludes.push(to_regex(&to_url(str).unwrap()));
                    } else {
                        supports.push((to_regex(&to_url(&string).unwrap()), string.len()));
                        
                        if let Some(host) = to_url(&string).unwrap().host()  {
                            let host = host.to_owned();
                            if let Some(vec) = host_map.get_mut(&host) {
                                vec.push(PluginId(id));
                            } else {
                                host_map.insert(host, vec![PluginId(id)]);
                            }
                        }
                    }
                }
            }

            if is_complex {
                complex_plugins.push(PluginId(id));
            }

            id += 1;
            Ok((supports, excludes, priority, bytecode))
        });

        let (supports, excludes, priority, bytecode) = result.unwrap();

        entries.push(RegistryEntry {
            plugin_path: path.to_str().unwrap().to_owned(),
            plugin_name: name,
            bytecode: Arc::new(bytecode),
            supports,
            excludes,
            priority,
        });
    }

    info!(count = entries.len(), "Loaded all plugins");

    (entries, host_map, complex_plugins)
}

fn to_url(str: &str) -> Result<Url, ParseError> {
    match Url::parse(str) {
        Err(ParseError::RelativeUrlWithoutBase) => {
            Url::parse(&format!("https://{}", str))
        },
        result => result,
    }
}

fn to_regex(url: &Url) -> Regex {
    // 1. Protocol: make the 's' in https optional
    let protocol = "https?";

    // 2. Host: Handle www. separately to make it optional
    let mut host = url.host_str().unwrap_or("").to_string();
    let host_pattern = if host.starts_with("www.") {
        host = host.replacen("www.", "", 1);
        format!(r"(www\.)?{}", escape(&host))
    } else {
        format!(r"(www\.)?{}", escape(&host))
    };

    // 3. Path: Escape the path
    // We trim the trailing slash if it exists to handle the "sub-path" logic manually
    let path = url.path().trim_end_matches('/');
    let escaped_path = escape(path);

    Regex::new(&format!(r"^{}://{}{}(?:/.*)?$", protocol, host_pattern, escaped_path)).expect("Failed to compile regex")
    
}
