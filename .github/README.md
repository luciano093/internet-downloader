

# Internet Downloader

A fast, cross-platform download manager built with a Rust backend and a React frontend. 

This project aims to be a modern, lightweight alternative to tools like JDownloader2 or IDM. The backend and frontend are decoupled, meaning the download engine can be run entirely headless on a server or NAS and controlled via the web UI.

<img width="1878" height="969" alt="Image" src="https://github.com/user-attachments/assets/8e32ce25-fd26-4cf3-b769-fba756e65e3c" />

## Current Status: Early Alpha
The core download engine is mostly stable and can handle complex downloads (it has been tested with 5k+ file downloads without any major problems), but the project is still in early development. Many UI elements are placeholders, basic configuration options are still being wired up, and the database schema may change without notice.

## Roadmap

### Plugin System
- [ ] Hot reloading for plugins during development
- [ ] Browser automation API via WebDriver (no bundled browser)
- [ ] Captcha relay to frontend for manual solving

### Download Engine
- [ ] Custom save directories per download
- [ ] Custom download categories with default paths per category

### Frontend
- [ ] Settings page
- [ ] Plugin management UI (install, enable/disable, configure, logs)

### Utils
- [ ] Extraction support (zip, rar, 7z) after download

## Features
* **Multi-part downloading**: files are split in multiple pieces to download many parts of the file in parallel
* **Multi-host downloads**: downloads can span one or multiple hosts simultaneously (In the future, this will make mirror support for single download possible)
* **JS plugin system**: plugins can be written in JavaScript (powered by rquickjs) to resolve URLs, add custom scraping, and extend site support
* **Throttling**: speed limits can be set globally, per host, per download, or per file (Backend-only at the moment)
* **Crash-safe persistence**: state is safely persisted to SQLite to survive crashes or restarts
* **Headless-capable**: the Rust backend can run independently of the React frontend. 
* **Chunked hash verification**: BLAKE3 hashing detects corruption, truncation, or bit-rot inline during download, re-downloading only broken chunks

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
