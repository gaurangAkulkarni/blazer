<div align="center">

<img src="docs/logo.svg" width="160" alt="Blazer Studio"/>

# Blazer Studio

**AI-powered local data studio for Mac**

Chat with your data · Run DuckDB SQL · Build agentic analysis pipelines

[![License: MIT](https://img.shields.io/badge/License-MIT-black.svg)](LICENSE)
[![Platform: macOS](https://img.shields.io/badge/Platform-macOS-black.svg)](https://github.com/gaurangAkulkarni/blazer/releases)
[![Built with Tauri](https://img.shields.io/badge/Built%20with-Tauri%202-black.svg)](https://tauri.app)
[![DuckDB](https://img.shields.io/badge/Engine-DuckDB-black.svg)](https://duckdb.org)

[Website](https://gaurangAkulkarni.github.io/blazer) · [Download](https://github.com/gaurangAkulkarni/blazer/releases) · [Report an issue](https://github.com/gaurangAkulkarni/blazer/issues)

</div>

---

## What is Blazer Studio?

Blazer Studio is a free, open-source desktop app that lets you explore and analyse data files using natural language and SQL — entirely on your machine. No cloud, no accounts, no telemetry.

Load a Parquet, CSV, or Excel file and ask questions in plain English. The AI writes the SQL, runs it, reads the actual results, and gives you a grounded answer. Or switch to the SQL console and write queries directly.

---

## Features

- **AI Chat** — Ask questions in plain English. Supports Anthropic Claude, OpenAI, and local Ollama models. Streams responses with live query execution.
- **DuckDB Console** — Full SQL editor with syntax highlighting, query history, snippet library, and schema explorer.
- **Agentic Mode** — Describe a goal. The agent plans steps, runs queries, reads the data, adapts to findings, and delivers a full written assessment.
- **Result Pane** — Every query result opens in a dedicated pane with table view, charts, column stats, and CSV export.
- **Schema Explorer** — Auto-detects columns, types, and cardinality from loaded files. Column-level profiling built in.
- **Multi-provider LLM** — Anthropic, OpenAI, Ollama, or any OpenAI-compatible endpoint via custom base URL.
- **Privacy first** — All data and queries stay local. Your files never leave your machine.

---

## Engine Status

| Engine | Status | Notes |
|---|---|---|
| **DuckDB** | ✅ Stable — primary focus | Parquet, CSV, XLSX, JSON, directories |
| **Blazer** | 🚧 Work in progress | MLX-based, Apple Silicon only — not yet production-ready |

> The Blazer engine is a ground-up columnar compute engine built on [Apple MLX](https://github.com/ml-explore/mlx), designed for GPU-accelerated data operations on Apple Silicon (M1/M2/M3/M4). Core operators are scaffolded. Python and Node.js bindings are available for early experimentation. Active development is focused on the DuckDB engine for now.

---

## Getting Started

### Prerequisites

- macOS 12+ (Apple Silicon or Intel)
- [Rust](https://rustup.rs) toolchain
- [Node.js](https://nodejs.org) 18+
- [Tauri CLI](https://tauri.app/start/prerequisites/) v2

### Run in development

```bash
git clone https://github.com/gaurangAkulkarni/blazer.git
cd blazer/playground
npm install --prefix ui
cargo tauri dev
```

### Build for release

```bash
cd blazer/playground
cargo tauri build
```

The `.dmg` / `.app` will be in `playground/target/release/bundle/`.

---

## Using the App

1. **Load a file** — Click the paperclip icon or drag a `.parquet`, `.csv`, or `.xlsx` file into the chat.
2. **Ask a question** — Type in the chat input. The AI will generate SQL, run it, and answer using the actual data.
3. **Run SQL directly** — Switch to the Console tab and write DuckDB SQL against your files.
4. **Agentic analysis** — Toggle the robot icon in the toolbar. Describe your analytical goal and the agent will plan and execute a full multi-step analysis.
5. **Configure LLM** — Open Settings (⌘,) to enter your API key and choose a provider/model.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Desktop framework | [Tauri 2](https://tauri.app) (Rust) |
| Query engine | [DuckDB](https://duckdb.org) via `duckdb` crate |
| Blazer engine | [Apple MLX](https://github.com/ml-explore/mlx) (WIP) |
| Frontend | React + TypeScript + Tailwind CSS |
| LLM providers | Anthropic, OpenAI, Ollama |
| Node.js bindings | [napi-rs](https://napi.rs) |
| Python bindings | [PyO3](https://pyo3.rs) |

---

## Project Structure

```
blazer/
├── crates/
│   ├── engine/          # Blazer columnar engine (WIP, MLX-based)
│   ├── mlx-sys/         # Rust bindings to Apple MLX
│   ├── node-bindings/   # Node.js (napi-rs) bindings
│   └── py-bindings/     # Python (PyO3) bindings
├── playground/          # Tauri desktop app
│   ├── src/             # Rust backend (Tauri commands)
│   └── ui/              # React/TypeScript frontend
└── docs/                # GitHub Pages website
```

---

## Roadmap

- [ ] Blazer engine: complete core SQL operators on MLX
- [ ] Blazer engine: streaming execution for large datasets
- [ ] Windows / Linux support (DuckDB engine)
- [ ] Plugin/skill system for custom LLM prompts
- [ ] GitHub Releases with pre-built `.dmg`

---

## Contributing

Issues and PRs are welcome. This is an independent personal open-source project — if you find it useful, a ⭐ goes a long way.

---

## License

[MIT](LICENSE) © 2025 [Gaurang Kulkarni](https://github.com/gaurangAkulkarni)

> Blazer Studio is an independent open-source project and is not affiliated with Kvell.
