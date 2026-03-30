mod commands;

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![
            commands::query::run_query,
            commands::query::get_schema,
            commands::files::open_file_dialog,
            commands::files::open_folder_dialog,
            commands::files::convert_to_parquet,
            commands::settings::load_settings,
            commands::settings::save_settings,
        ])
        .run(tauri::generate_context!())
        .expect("error while running blazer");
}
