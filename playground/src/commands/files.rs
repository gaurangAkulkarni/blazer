use blazer_engine::io::{CsvReader, ParquetWriter};
use serde::Serialize;
use std::path::Path;
use tauri::AppHandle;
use tauri_plugin_dialog::DialogExt;

#[derive(Serialize)]
pub struct FileInfo {
    pub path: String,
    pub name: String,
    pub ext: String,
    pub columns: Option<Vec<String>>,
}

#[tauri::command]
pub async fn open_file_dialog(app: AppHandle) -> Result<Vec<FileInfo>, String> {
    let paths = app
        .dialog()
        .file()
        .add_filter("Data files", &["csv", "tsv", "parquet"])
        .blocking_pick_files();

    let paths = match paths {
        Some(p) => p,
        None => return Ok(vec![]),
    };

    let mut results = Vec::new();
    for file_path in paths {
        let path_str = file_path.to_string();
        let p = Path::new(&path_str);
        let name = p
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        let ext = p
            .extension()
            .map(|e| e.to_string_lossy().to_lowercase())
            .unwrap_or_default();

        // For CSV/TSV, read the header row to get column names
        let columns = if ext == "csv" || ext == "tsv" {
            read_csv_header(&path_str).ok()
        } else {
            None
        };

        results.push(FileInfo {
            path: path_str,
            name,
            ext,
            columns,
        });
    }

    Ok(results)
}

#[tauri::command]
pub async fn open_folder_dialog(app: AppHandle) -> Result<Option<FileInfo>, String> {
    let folder = app.dialog().file().blocking_pick_folder();

    match folder {
        None => Ok(None),
        Some(folder_path) => {
            let path_str = folder_path.to_string();
            let p = Path::new(&path_str);
            let name = p
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            Ok(Some(FileInfo {
                path: path_str,
                name,
                ext: "parquet_dir".to_string(),
                columns: None,
            }))
        }
    }
}

#[tauri::command]
pub async fn convert_to_parquet(csv_path: String) -> Result<String, String> {
    let input = Path::new(&csv_path);
    let output_path = input.with_extension("parquet");
    let output_str = output_path.to_string_lossy().to_string();

    // Read CSV and write Parquet
    let df = CsvReader::from_path(&csv_path)
        .map_err(|e| format!("CSV open error: {e}"))?
        .finish()
        .map_err(|e| format!("CSV parse error: {e}"))?;

    ParquetWriter::from_path(&output_str)
        .finish(&df)
        .map_err(|e| format!("Parquet write error: {e}"))?;

    Ok(output_str)
}

// Read just the first row of a CSV to get column names
fn read_csv_header(path: &str) -> Result<Vec<String>, String> {
    let df = CsvReader::from_path(path)
        .map_err(|e| e.to_string())?
        .finish()
        .map_err(|e| e.to_string())?;

    Ok(df
        .get_column_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect())
}
