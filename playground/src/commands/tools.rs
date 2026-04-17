use crate::commands::duckdb::execute_sql_on_conn;
use duckdb::Connection;
use serde_json::{json, Value};
use std::time::Instant;

pub fn dispatch_tool_call(name: &str, arguments: Value) -> Value {
    match name {
        "run_sql"         => handle_run_sql(arguments),
        "describe_tables" => handle_describe_tables(arguments),
        "get_sample_rows" => handle_get_sample_rows(arguments),
        "column_stats"    => handle_column_stats(arguments),
        "export_result"   => handle_export_result(arguments),
        _ => json!({"success": false, "error": format!("Unknown tool: {}", name)}),
    }
}

fn open_conn() -> Result<Connection, String> {
    Connection::open_in_memory().map_err(|e| format!("DuckDB connection error: {e}"))
}

/// Open a fresh in-memory DuckDB connection and immediately recreate a VIEW for
/// every file in the `files` array so that alias-style queries (e.g. `FROM tracker`)
/// work without the LLM ever needing to type a full path.
///
/// Each file entry is expected to have:
///   { alias: "tracker", reader: "read_parquet('/...')" }
/// (or falls back to path+ext for backward compat)
fn open_conn_with_views(args: &Value) -> Result<Connection, String> {
    let conn = Connection::open_in_memory()
        .map_err(|e| format!("DuckDB connection error: {e}"))?;

    if let Some(files) = args["files"].as_array() {
        for file in files {
            let alias = match file["alias"].as_str() {
                Some(a) if !a.is_empty() => a.to_string(),
                _ => continue,
            };
            // Prefer the pre-computed reader; fall back to path+ext
            let reader = if let Some(r) = file["reader"].as_str().filter(|s| !s.is_empty()) {
                r.to_string()
            } else if let (Some(path), Some(ext)) = (file["path"].as_str(), file["ext"].as_str()) {
                read_expr_for_ext(path, ext)
            } else {
                continue;
            };

            // CREATE OR REPLACE VIEW — ignore errors (e.g. bad paths at boot time)
            let view_sql = format!("CREATE OR REPLACE VIEW \"{alias}\" AS SELECT * FROM {reader}");
            let _ = conn.execute_batch(&view_sql);
        }
    }

    Ok(conn)
}

fn sanitize_table_ref(table: &str) -> String {
    let t = table.trim();
    // Already a reader function call — use as-is to avoid double-wrapping
    // (LLMs often pass "read_parquet('/path/**/*.parquet')" as the table argument)
    if t.starts_with("read_parquet(")
        || t.starts_with("read_csv(")
        || t.starts_with("read_csv_auto(")
        || t.starts_with("read_xlsx(")
        || t.starts_with("scan_parquet(")
    {
        return t.to_string();
    }
    if t.ends_with(".parquet") || t.contains("*.parquet") || t.contains("**") {
        return format!("read_parquet('{}')", t.replace('\'', ""));
    }
    if t.ends_with(".csv") || t.ends_with(".tsv") {
        return format!("read_csv_auto('{}')", t.replace('\'', ""));
    }
    if t.ends_with(".xlsx") {
        return format!("read_xlsx('{}', all_varchar=true)", t.replace('\'', ""));
    }
    // Bare filesystem path that is a directory → treat as a partitioned parquet_dir
    if std::path::Path::new(t).is_dir() {
        return format!("read_parquet('{}/**/*.parquet')", t.replace('\'', ""));
    }
    format!("\"{}\"", t.replace('"', ""))
}

/// Build the correct DuckDB reader expression using the known file ext
/// (mirrors the TypeScript readExpr function in readExpr.ts).
fn read_expr_for_ext(path: &str, ext: &str) -> String {
    let p = path.replace('\'', "''");
    match ext.to_lowercase().as_str() {
        "parquet_dir" => format!("read_parquet('{p}/**/*.parquet')"),
        "parquet"     => format!("read_parquet('{p}')"),
        "csv" | "tsv" => format!("read_csv_auto('{p}')"),
        "xlsx"        => format!("read_xlsx('{p}', all_varchar=true)"),
        "xlsx_dir"    => format!("read_xlsx('{p}/*.xlsx', all_varchar=true)"),
        "csv_dir"     => format!("read_csv_auto('{p}/*.csv')"),
        _             => sanitize_table_ref(path),
    }
}

fn handle_run_sql(args: Value) -> Value {
    let sql = match args["sql"].as_str() {
        Some(s) => s.to_string(),
        None => return json!({"success": false, "error": "Missing required argument: sql"}),
    };
    let limit = args["limit"].as_u64().unwrap_or(100) as usize;
    let upper = sql.trim_start().to_uppercase();
    let is_select = upper.starts_with("SELECT") || upper.starts_with("WITH");
    let has_limit = upper.contains("LIMIT");
    let sql_executed = if is_select && !has_limit {
        format!("{} LIMIT {}", sql.trim_end_matches(';'), limit)
    } else {
        sql.clone()
    };
    // Use open_conn_with_views so alias-style queries (FROM tracker) work
    let conn = match open_conn_with_views(&args) {
        Ok(c) => c,
        Err(e) => return json!({"success": false, "error": e, "sql_attempted": sql}),
    };
    let start = Instant::now();
    match execute_sql_on_conn(&conn, &sql_executed) {
        Ok((rows, cols)) => {
            let total = rows.len();
            let truncated = is_select && !has_limit && total >= limit;
            json!({
                "success": true,
                "columns": cols,
                "rows": rows,
                "row_count": total,
                "truncated": truncated,
                "sql_executed": sql_executed,
                "duration_ms": start.elapsed().as_millis() as u64,
            })
        }
        Err(e) => json!({"success": false, "error": e, "sql_attempted": sql_executed}),
    }
}

/// For a directory path, glob its contents and return a DuckDB reader expression
/// that matches the dominant file type actually found inside.
/// Returns None if the directory is empty or unreadable.
fn detect_dir_reader(conn: &Connection, dir_path: &str) -> Option<String> {
    let p = dir_path.replace('\'', "''");
    let glob_sql = format!(
        "SELECT file FROM glob('{p}/**/*') \
         WHERE file NOT LIKE '%.DS_Store' AND file NOT LIKE '%/.git/%' LIMIT 100"
    );
    let (rows, _) = execute_sql_on_conn(conn, &glob_sql).ok()?;
    if rows.is_empty() { return None; }

    // Tally file extensions
    let mut ext_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut file_paths: Vec<String> = Vec::new();
    for row in &rows {
        let f = row["file"].as_str().unwrap_or("").to_string();
        if f.is_empty() { continue; }
        let ext = f.rsplit('.').next().unwrap_or("").to_lowercase();
        if !ext.is_empty() && ext.len() <= 8 {
            *ext_counts.entry(ext.clone()).or_insert(0) += 1;
        }
        file_paths.push(f);
    }

    let dominant = ext_counts.into_iter().max_by_key(|(_, v)| *v)?.0;
    let p2 = dir_path.replace('\'', "''");

    match dominant.as_str() {
        "parquet" => Some(format!("read_parquet('{p2}/**/*.parquet')")),
        "csv"     => Some(format!("read_csv_auto('{p2}/*.csv')")),
        "tsv"     => Some(format!("read_csv_auto('{p2}/*.tsv')")),
        "xlsx"    => Some(format!("read_xlsx('{p2}/*.xlsx', all_varchar=true)")),
        "json" | "ndjson" | "jsonl" => {
            let json_files: Vec<&str> = file_paths.iter()
                .map(String::as_str)
                .filter(|f| {
                    let e = f.rsplit('.').next().unwrap_or("").to_lowercase();
                    matches!(e.as_str(), "json" | "ndjson" | "jsonl")
                })
                .collect();
            if json_files.len() == 1 {
                let fp = json_files[0].replace('\'', "''");
                Some(format!("read_json_auto('{fp}')"))
            } else {
                Some(format!("read_json_auto('{p2}/**/*.{dominant}')"))
            }
        }
        _ => None,
    }
}

/// Extracts the file path string from a DuckDB reader expression.
/// e.g. "read_json_auto('/path/file.ndjson')" → Some("/path/file.ndjson")
fn extract_path_from_reader(expr: &str) -> Option<&str> {
    let start = expr.find('\'')?  + 1;
    let end   = expr.rfind('\'')?;
    if end > start { Some(&expr[start..end]) } else { None }
}

/// Returns true when the ext or path indicates this is a directory-style source.
fn is_directory_ext(ext: &str) -> bool {
    matches!(ext, "" | "parquet_dir" | "csv_dir" | "xlsx_dir" | "json_dir" | "ndjson_dir")
}

fn handle_describe_tables(args: Value) -> Value {
    let conn = match open_conn() {
        Ok(c) => c,
        Err(e) => return json!({"success": false, "error": e}),
    };

    // When the JS side injects attached file paths, describe each file directly
    // using DuckDB's DESCRIBE command. This works for parquet/csv/xlsx path-based
    // files (which never appear in information_schema.columns for a fresh connection).
    if let Some(files) = args["files"].as_array() {
        let mut tables: Vec<Value> = Vec::new();
        for file in files {
            let path = match file["path"].as_str() {
                Some(p) => p,
                None => continue,
            };
            // Use ext when available (injected from AttachedFile) for accurate reader selection.
            // Falls back to sanitize_table_ref (which now also handles bare directory paths).
            let ext = file["ext"].as_str().unwrap_or("");
            let safe_ref = if ext.is_empty() {
                sanitize_table_ref(path)
            } else {
                read_expr_for_ext(path, ext)
            };

            let push_columns = |tables: &mut Vec<Value>, rows: Vec<serde_json::Map<String, Value>>, actual_path: &str| {
                let cols: Vec<Value> = rows.iter().map(|r| json!({
                    "name": r["column_name"],
                    "type": r["column_type"],
                })).collect();
                let col_count = cols.len();
                tables.push(json!({
                    "name": actual_path,
                    "columns": cols,
                    "column_count": col_count,
                }));
            };

            let describe_sql = format!("DESCRIBE SELECT * FROM {} LIMIT 0", safe_ref);
            match execute_sql_on_conn(&conn, &describe_sql) {
                Ok((rows, _)) => push_columns(&mut tables, rows, path),
                Err(_) => {
                    // For directory-type sources, the default reader may be wrong
                    // (e.g. parquet_dir assumed for a folder that actually has NDJSON).
                    // Glob the directory to detect the actual format and retry.
                    if is_directory_ext(ext) || std::path::Path::new(path).is_dir() {
                        if let Some(alt_ref) = detect_dir_reader(&conn, path) {
                            let alt_sql = format!("DESCRIBE SELECT * FROM {} LIMIT 0", alt_ref);
                            if let Ok((rows, _)) = execute_sql_on_conn(&conn, &alt_sql) {
                                // Use the specific file path from the reader as the table name
                                // so the JS caching layer can match it back to the parent folder.
                                let actual = extract_path_from_reader(&alt_ref).unwrap_or(path);
                                push_columns(&mut tables, rows, actual);
                            }
                        }
                    }
                    // else: file inaccessible or unrecognised format — skip silently
                }
            }
        }
        return json!({"success": true, "tables": tables});
    }

    // Fallback: query information_schema for any registered in-memory views/tables
    let sql = "SELECT table_name, column_name, data_type \
               FROM information_schema.columns \
               ORDER BY table_name, ordinal_position";
    match execute_sql_on_conn(&conn, sql) {
        Ok((rows, _)) => {
            let mut tables: std::collections::BTreeMap<String, Vec<Value>> = std::collections::BTreeMap::new();
            for row in &rows {
                let tname = row["table_name"].as_str().unwrap_or("").to_string();
                tables.entry(tname).or_default().push(json!({
                    "name": &row["column_name"],
                    "type": &row["data_type"],
                }));
            }
            let list: Vec<Value> = tables.into_iter().map(|(name, cols)| {
                let n = cols.len();
                json!({"name": name, "columns": cols, "column_count": n})
            }).collect();
            json!({"success": true, "tables": list})
        }
        Err(e) => json!({"success": false, "error": e}),
    }
}

fn handle_get_sample_rows(args: Value) -> Value {
    let table = match args["table"].as_str() {
        Some(t) => t.to_string(),
        None => return json!({"success": false, "error": "Missing required argument: table"}),
    };
    let n = args["n"].as_u64().unwrap_or(10);
    let safe_table = sanitize_table_ref(&table);
    let sql = format!("SELECT * FROM {} LIMIT {}", safe_table, n);
    // Forward files so the new connection has the same alias views
    let mut inner = json!({"sql": sql, "limit": n});
    if let Some(files) = args.get("files") {
        inner["files"] = files.clone();
    }
    handle_run_sql(inner)
}

fn handle_column_stats(args: Value) -> Value {
    let table = match args["table"].as_str() {
        Some(t) => t.to_string(),
        None => return json!({"success": false, "error": "Missing required argument: table"}),
    };
    let conn = match open_conn_with_views(&args) {
        Ok(c) => c,
        Err(e) => return json!({"success": false, "error": e}),
    };
    let safe_table = sanitize_table_ref(&table);

    // Resolve columns to profile
    let columns: Vec<String> = if let Some(arr) = args["columns"].as_array() {
        arr.iter().filter_map(|v| v.as_str().map(String::from)).collect()
    } else {
        match execute_sql_on_conn(&conn, &format!("DESCRIBE SELECT * FROM {} LIMIT 0", safe_table)) {
            Ok((rows, _)) => rows.iter()
                .filter_map(|r| r["column_name"].as_str().map(String::from))
                .collect(),
            Err(_) => vec![],
        }
    };

    if columns.is_empty() {
        return json!({"success": true, "table": table, "column_profiles": []});
    }

    // ── Single combined stats query ───────────────────────────────────────────
    // Instead of one full-table scan per column (O(N) scans), we run ONE scan
    // that computes COUNT / DISTINCT / MIN / MAX / AVG / MEDIAN for every column
    // simultaneously. Named aliases use numeric suffixes to stay SQL-safe with
    // any column name.
    let agg_exprs: Vec<String> = columns.iter().enumerate().map(|(i, col)| {
        let c = format!("\"{}\"", col.replace('"', ""));
        format!(
            "COUNT({c}) AS cnt{i}, \
             COUNT(DISTINCT {c}) AS dst{i}, \
             MIN({c})::VARCHAR AS mn{i}, \
             MAX({c})::VARCHAR AS mx{i}, \
             AVG(TRY_CAST({c} AS DOUBLE))::VARCHAR AS avg{i}, \
             MEDIAN(TRY_CAST({c} AS DOUBLE))::VARCHAR AS med{i}",
            c = c, i = i
        )
    }).collect();

    let combined_sql = format!(
        "SELECT COUNT(*) AS _total, {} FROM {}",
        agg_exprs.join(", "),
        safe_table
    );

    let stats_row = match execute_sql_on_conn(&conn, &combined_sql) {
        Ok((rows, _)) if !rows.is_empty() => rows.into_iter().next().unwrap(),
        Ok(_) => return json!({"success": true, "table": table, "column_profiles": []}),
        Err(e) => return json!({"success": false, "error": e}),
    };

    let total = stats_row["_total"].as_u64().unwrap_or(0);

    // ── Per-column top-values (cheap GROUP BY queries) ────────────────────────
    let mut profiles: Vec<Value> = Vec::new();
    for (i, col) in columns.iter().enumerate() {
        let safe_col = format!("\"{}\"", col.replace('"', ""));

        let cnt_key  = format!("cnt{}", i);
        let dst_key  = format!("dst{}", i);
        let mn_key   = format!("mn{}", i);
        let mx_key   = format!("mx{}", i);
        let avg_key  = format!("avg{}", i);
        let med_key  = format!("med{}", i);

        let cnt = stats_row.get(cnt_key.as_str()).and_then(|v| v.as_u64()).unwrap_or(0);
        let null_count = total.saturating_sub(cnt);
        let null_pct = if total > 0 {
            (null_count as f64 / total as f64 * 1000.0).round() / 10.0
        } else { 0.0 };

        let top_sql = format!(
            "SELECT {col}::VARCHAR AS value, COUNT(*) AS freq \
             FROM {tbl} WHERE {col} IS NOT NULL \
             GROUP BY {col} ORDER BY freq DESC LIMIT 5",
            col = safe_col, tbl = safe_table
        );
        let top_values: Vec<Value> = match execute_sql_on_conn(&conn, &top_sql) {
            Ok((rows, _)) => rows.iter()
                .map(|r| json!({"value": r["value"], "frequency": r["freq"]}))
                .collect(),
            _ => vec![],
        };

        profiles.push(json!({
            "column":         col,
            "total_count":    total,
            "null_count":     null_count,
            "null_percentage": null_pct,
            "distinct_count": stats_row.get(dst_key.as_str()),
            "min":            stats_row.get(mn_key.as_str()),
            "max":            stats_row.get(mx_key.as_str()),
            "mean":           stats_row.get(avg_key.as_str()),
            "median":         stats_row.get(med_key.as_str()),
            "top_values":     top_values,
        }));
    }
    json!({"success": true, "table": table, "column_profiles": profiles})
}

fn handle_export_result(args: Value) -> Value {
    let format = match args["format"].as_str() {
        Some(f) => f.to_string(),
        None => return json!({"success": false, "error": "Missing required argument: format"}),
    };
    let filename = match args["filename"].as_str() {
        Some(f) => f.to_string(),
        None => return json!({"success": false, "error": "Missing required argument: filename"}),
    };
    let sql = match args["sql"].as_str() {
        Some(s) => s.to_string(),
        None => return json!({"success": false, "error": "Missing required argument: sql (provide the query to export)"}),
    };
    let safe_name: String = filename.chars()
        .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-' || *c == '.')
        .collect();
    if safe_name.is_empty() {
        return json!({"success": false, "error": "Invalid filename"});
    }
    let downloads = dirs::download_dir().unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
    let path_str = downloads.join(&safe_name).to_string_lossy().to_string();
    let fmt_str = match format.as_str() {
        "csv"     => "CSV, HEADER TRUE",
        "parquet" => "PARQUET",
        "json"    => "JSON",
        other     => return json!({"success": false, "error": format!("Unsupported format: {other}")}),
    };
    let copy_sql = format!(
        "COPY ({}) TO '{}' (FORMAT {})",
        sql.trim_end_matches(';'),
        path_str.replace('\'', "''"),
        fmt_str
    );
    let conn = match open_conn() {
        Ok(c) => c,
        Err(e) => return json!({"success": false, "error": e}),
    };
    match conn.execute_batch(&copy_sql) {
        Ok(_) => json!({"success": true, "path": path_str, "format": format}),
        Err(e) => json!({"success": false, "error": e.to_string()}),
    }
}

#[tauri::command]
pub async fn execute_tool_call(name: String, arguments: Value) -> Value {
    tokio::task::spawn_blocking(move || dispatch_tool_call(&name, arguments))
        .await
        .unwrap_or_else(|e| json!({"success": false, "error": format!("Task panicked: {e}")}))
}
