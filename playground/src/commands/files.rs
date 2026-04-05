use blazer_engine::io::{CsvReader, ParquetReader, ParquetWriter};
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
        .add_filter("Data files", &["csv", "tsv", "parquet", "xlsx", "xls"])
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

            // Detect dominant file type in the folder
            let dir_ext = detect_folder_type(&path_str);

            // Read column names only for parquet dirs
            let columns = if dir_ext == "parquet_dir" {
                read_parquet_dir_columns(&path_str).ok()
            } else {
                None
            };

            Ok(Some(FileInfo {
                path: path_str,
                name,
                ext: dir_ext,
                columns,
            }))
        }
    }
}

/// Detect the dominant file type in a folder by counting extensions.
/// Returns "xlsx_dir", "csv_dir", or "parquet_dir".
fn detect_folder_type(dir: &str) -> String {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return "parquet_dir".to_string();
    };

    let mut xlsx = 0usize;
    let mut csv = 0usize;
    let mut parquet = 0usize;

    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_dir() { continue; }
        match path.extension().and_then(|e| e.to_str()).map(|e| e.to_lowercase()).as_deref() {
            Some("xlsx") | Some("xls") => xlsx += 1,
            Some("csv") | Some("tsv")  => csv += 1,
            Some("parquet")            => parquet += 1,
            _ => {}
        }
    }

    if xlsx >= csv && xlsx >= parquet && xlsx > 0 {
        "xlsx_dir".to_string()
    } else if csv >= parquet && csv > 0 {
        "csv_dir".to_string()
    } else {
        "parquet_dir".to_string()
    }
}

/// Read column names from the schema of the first .parquet file in a directory.
fn read_parquet_dir_columns(dir: &str) -> Result<Vec<String>, String> {
    let entries = std::fs::read_dir(dir).map_err(|e| e.to_string())?;
    let first_parquet = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"));

    let file = first_parquet.ok_or("No parquet files found in directory")?;

    let df = ParquetReader::from_path(&file)
        .map_err(|e| e.to_string())?
        .with_n_rows(0)
        .finish()
        .map_err(|e| e.to_string())?;

    Ok(df
        .get_column_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect())
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
