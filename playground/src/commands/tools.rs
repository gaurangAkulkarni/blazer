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

fn sanitize_table_ref(table: &str) -> String {
    if table.ends_with(".parquet") || table.contains("*.parquet") {
        return format!("read_parquet('{}')", table.replace('\'', ""));
    }
    if table.ends_with(".csv") || table.ends_with(".tsv") {
        return format!("read_csv_auto('{}')", table.replace('\'', ""));
    }
    if table.ends_with(".xlsx") {
        return format!("read_xlsx('{}', all_varchar=true)", table.replace('\'', ""));
    }
    format!("\"{}\"", table.replace('"', ""))
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
    let conn = match open_conn() {
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

fn handle_describe_tables(_args: Value) -> Value {
    let conn = match open_conn() {
        Ok(c) => c,
        Err(e) => return json!({"success": false, "error": e}),
    };
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
    handle_run_sql(json!({"sql": sql, "limit": n}))
}

fn handle_column_stats(args: Value) -> Value {
    let table = match args["table"].as_str() {
        Some(t) => t.to_string(),
        None => return json!({"success": false, "error": "Missing required argument: table"}),
    };
    let conn = match open_conn() {
        Ok(c) => c,
        Err(e) => return json!({"success": false, "error": e}),
    };
    let safe_table = sanitize_table_ref(&table);

    // Resolve columns to profile
    let columns: Vec<String> = if let Some(arr) = args["columns"].as_array() {
        arr.iter().filter_map(|v| v.as_str().map(String::from)).collect()
    } else {
        match execute_sql_on_conn(&conn, &format!("DESCRIBE {}", safe_table)) {
            Ok((rows, _)) => rows.iter()
                .filter_map(|r| r["column_name"].as_str().map(String::from))
                .collect(),
            Err(_) => vec![],
        }
    };

    let mut profiles: Vec<Value> = Vec::new();
    for col in &columns {
        let safe_col = format!("\"{}\"", col.replace('"', ""));
        let stats_sql = format!(
            "SELECT COUNT(*) AS total_count, \
             COUNT(*) - COUNT({col}) AS null_count, \
             COUNT(DISTINCT {col}) AS distinct_count, \
             MIN({col})::VARCHAR AS min_val, \
             MAX({col})::VARCHAR AS max_val, \
             AVG(TRY_CAST({col} AS DOUBLE))::VARCHAR AS mean_val, \
             MEDIAN(TRY_CAST({col} AS DOUBLE))::VARCHAR AS median_val \
             FROM {tbl}",
            col = safe_col, tbl = safe_table
        );
        let stats = match execute_sql_on_conn(&conn, &stats_sql) {
            Ok((rows, _)) if !rows.is_empty() => rows.into_iter().next().unwrap(),
            _ => continue,
        };
        let top_sql = format!(
            "SELECT {col}::VARCHAR AS value, COUNT(*) AS freq \
             FROM {tbl} WHERE {col} IS NOT NULL \
             GROUP BY {col} ORDER BY freq DESC LIMIT 5",
            col = safe_col, tbl = safe_table
        );
        let top_values: Vec<Value> = match execute_sql_on_conn(&conn, &top_sql) {
            Ok((rows, _)) => rows.iter().map(|r| json!({"value": r["value"], "frequency": r["freq"]})).collect(),
            _ => vec![],
        };
        let total = stats["total_count"].as_u64().unwrap_or(0);
        let nulls = stats["null_count"].as_u64().unwrap_or(0);
        let null_pct = if total > 0 { (nulls as f64 / total as f64 * 1000.0).round() / 10.0 } else { 0.0 };
        profiles.push(json!({
            "column": col,
            "total_count": total,
            "null_count": nulls,
            "null_percentage": null_pct,
            "distinct_count": stats["distinct_count"],
            "min": stats["min_val"],
            "max": stats["max_val"],
            "mean": stats["mean_val"],
            "median": stats["median_val"],
            "top_values": top_values,
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
pub fn execute_tool_call(name: String, arguments: Value) -> Value {
    dispatch_tool_call(&name, arguments)
}
