

# Internet Downloader

A fast, cross-platform download manager built with a Rust backend and a React frontend. 

This project aims to be a modern, lightweight alternative to tools like JDownloader2 or IDM. The backend and frontend are decoupled, meaning the download engine can be run entirely headless on a server or NAS and controlled via the web UI.

<img width="1878" height="969" alt="Image" src="https://github.com/user-attachments/assets/8e32ce25-fd26-4cf3-b769-fba756e65e3c" />

## Current Status: Early Alpha
The core download engine and real-time state synchronization are highly functional, but the project is still in early development. Many UI elements are currently placeholders, and basic configuration options are still being wired up. 

**Roadmap:**
* Build out the Settings page
* Implement dynamic save directory selection
* Hook up frontend sorting and filtering for the sidebar
* Refine the JS plugin system for link scraping

## Features
* Multi-part downloading
* Throttling. Speed limits can be set globally, per host, per download, or per file (Backend-only at the moment)
* Nice looking UI
* Downloads can be paused and resumed, and state is safely persisted to SQLite to survive crashes or restarts.
* Disk writes are decoupled into a dedicated thread pool to keep the core engine unblocked.
* The Rust backend can run independently of the React frontend.
* Uses BLAKE3 chunk hashing to detect file corruption, truncation, or bit-rot, seamlessly re-downloading only the broken or missing pieces.

## Local Development Setup

You will need Rust/Cargo and Node.js installed on your machine.

1. **Start the backend:**
   ```bash
   cd internet-downloader-backend
   cargo run
   ```

2. **Start the frontend:**
   ```bash
   cd internet-downloader-frontend
   pnpm install
   pnpm run dev
   ```

## License
This project is licensed under the GNU AGPLv3 License. See the LICENSE file for details.
