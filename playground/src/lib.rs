mod commands;

use tauri::Manager;

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_sql::Builder::new().build())
        .invoke_handler(tauri::generate_handler![
            commands::query::run_query,
            commands::query::get_schema,
            commands::files::open_file_dialog,
            commands::files::open_folder_dialog,
            commands::files::convert_to_parquet,
            commands::settings::load_settings,
            commands::settings::save_settings,
            commands::llm::stream_llm,
            commands::llm::fetch_openai_models,
            commands::duckdb::check_duckdb,
            commands::duckdb::install_duckdb,
            commands::duckdb::run_duckdb_query,
            commands::duckdb::list_duckdb_extensions,
            commands::duckdb::install_duckdb_extension,
            commands::duckdb::run_duckdb_query_with_connections,
            commands::duckdb::export_to_parquet,
        ])
        .setup(|app| {
            // Set app icon at runtime so dev builds also show the new logo.
            let icon = tauri::include_image!("icons/icon.png");
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.set_icon(icon);
            }
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running blazer");
}
