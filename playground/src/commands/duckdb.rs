use crate::commands::settings::ConnectionAlias;
use duckdb::types::Value as DuckValue;
use duckdb::Connection;
use serde_json::Value;
use std::io::Write;

#[derive(serde::Serialize)]
pub struct DuckDbResult {
    pub success: bool,
    pub error: Option<String>,
    pub data: Vec<serde_json::Map<String, Value>>,
    pub columns: Vec<String>,
    pub shape: [usize; 2],
    pub duration_ms: u64,
}

/// DuckDB is now bundled via duckdb-rs — always available, no external CLI needed.
#[tauri::command]
pub async fn check_duckdb() -> bool {
    true
}

/// No-op: duckdb-rs is statically linked; no external installation step is required.
#[tauri::command]
pub async fn install_duckdb() -> Result<String, String> {
    Ok("DuckDB is bundled with Blazer — no installation needed.".to_string())
}

/// Execute an arbitrary SQL statement via the embedded DuckDB engine and return results.
///
/// A fresh in-memory connection is opened per call.  DuckDB can still read files
/// from disk (e.g. `read_parquet('/path/*.parquet')`) even with an in-memory
/// connection — "in-memory" only means result tables aren't persisted.
#[tauri::command]
pub async fn run_duckdb_query(sql: String) -> DuckDbResult {
    let task = tokio::task::spawn_blocking(move || {
        let start = std::time::Instant::now();
        let result = execute_sql_native(&sql);
        let duration_ms = start.elapsed().as_millis() as u64;
        (result, duration_ms)
    });

    match task.await {
        Ok((Ok((data, columns)), duration_ms)) => {
            let shape = [data.len(), columns.len()];
            DuckDbResult {
                success: true,
                error: None,
                data,
                columns,
                shape,
                duration_ms,
            }
        }
        Ok((Err(e), duration_ms)) => DuckDbResult {
            success: false,
            error: Some(e),
            data: vec![],
            columns: vec![],
            shape: [0, 0],
            duration_ms,
        },
        Err(e) => DuckDbResult {
            success: false,
            error: Some(format!("Task error: {e}")),
            data: vec![],
            columns: vec![],
            shape: [0, 0],
            duration_ms: 0,
        },
    }
}

/// Run multiple SQL statements in a **single shared in-memory connection** so that
/// DDL from earlier blocks (CREATE VIEW, CREATE TABLE, etc.) is visible to later ones.
/// Returns one DuckDbResult per statement, in order.
#[tauri::command]
pub async fn run_duckdb_batch(sqls: Vec<String>) -> Vec<DuckDbResult> {
    tokio::task::spawn_blocking(move || {
        let conn = match Connection::open_in_memory() {
            Ok(c) => c,
            Err(e) => {
                let err = format!("DuckDB connection error: {e}");
                return sqls.iter().map(|_| DuckDbResult {
                    success: false,
                    error: Some(err.clone()),
                    data: vec![],
                    columns: vec![],
                    shape: [0, 0],
                    duration_ms: 0,
                }).collect();
            }
        };

        sqls.into_iter().map(|sql| {
            let start = std::time::Instant::now();
            match execute_sql_on_conn(&conn, &sql) {
                Ok((data, columns)) => DuckDbResult {
                    success: true,
                    error: None,
                    shape: [data.len(), columns.len()],
                    data,
                    columns,
                    duration_ms: start.elapsed().as_millis() as u64,
                },
                Err(e) => DuckDbResult {
                    success: false,
                    error: Some(e),
                    data: vec![],
                    columns: vec![],
                    shape: [0, 0],
                    duration_ms: start.elapsed().as_millis() as u64,
                },
            }
        }).collect()
    }).await.unwrap_or_default()
}

// ── Native execution ──────────────────────────────────────────────────────────

fn execute_sql_native(
    sql: &str,
) -> Result<(Vec<serde_json::Map<String, Value>>, Vec<String>), String> {
    let conn = Connection::open_in_memory()
        .map_err(|e| format!("DuckDB connection error: {e}"))?;
    execute_sql_on_conn(&conn, sql)
}

pub(crate) fn execute_sql_on_conn(
    conn: &Connection,
    sql: &str,
) -> Result<(Vec<serde_json::Map<String, Value>>, Vec<String>), String> {
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("SQL error: {e}"))?;

    let raw_rows: Vec<Vec<DuckValue>> = stmt
        .query_map([], |row| {
            let mut vals = Vec::new();
            for i in 0_usize.. {
                match row.get::<_, DuckValue>(i) {
                    Ok(v)  => vals.push(v),
                    Err(_) => break,
                }
            }
            Ok(vals)
        })
        .map_err(|e| format!("Query execution error: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Row collection error: {e}"))?;

    let column_names = stmt.column_names();

    let rows: Vec<serde_json::Map<String, Value>> = raw_rows
        .into_iter()
        .map(|vals| {
            column_names
                .iter()
                .zip(vals)
                .map(|(name, val)| (name.clone(), duck_to_json(val)))
                .collect()
        })
        .collect();

    Ok((rows, column_names))
}

// ── DuckDB Value → serde_json::Value ─────────────────────────────────────────

pub(crate) fn duck_to_json(val: DuckValue) -> Value {
    match val {
        DuckValue::Null => Value::Null,
        DuckValue::Boolean(b) => Value::Bool(b),

        // Integer types
        DuckValue::TinyInt(i) => serde_json::json!(i),
        DuckValue::SmallInt(i) => serde_json::json!(i),
        DuckValue::Int(i) => serde_json::json!(i),
        DuckValue::BigInt(i) => serde_json::json!(i),
        // i128 overflows JSON Number — emit as string
        DuckValue::HugeInt(i) => Value::String(i.to_string()),

        // Unsigned integer types
        DuckValue::UTinyInt(i) => serde_json::json!(i),
        DuckValue::USmallInt(i) => serde_json::json!(i),
        DuckValue::UInt(i) => serde_json::json!(i),
        DuckValue::UBigInt(i) => serde_json::json!(i),

        // Floating-point
        DuckValue::Float(f) => serde_json::json!(f as f64),
        DuckValue::Double(f) => serde_json::json!(f),

        // rust_decimal::Decimal — serialize as its canonical string representation
        // to avoid precision loss and because JSON has no fixed-point type.
        DuckValue::Decimal(d) => Value::String(d.to_string()),

        // Temporal: emit as raw i64 microseconds / days; frontend formats as needed
        DuckValue::Timestamp(_, micros) => serde_json::json!(micros),
        DuckValue::Date32(days) => serde_json::json!(days),
        DuckValue::Time64(_, nanos) => serde_json::json!(nanos),
        DuckValue::Interval { months, days, nanos } => {
            Value::String(format!("{months}mo {days}d {nanos}ns"))
        }

        // Text & binary
        DuckValue::Text(s) => Value::String(s),
        DuckValue::Blob(b) => Value::String(format!("[{} bytes]", b.len())),

        // Enum is stored as its string label
        DuckValue::Enum(s) => Value::String(s),

        // Container types — recurse
        DuckValue::List(items) | DuckValue::Array(items) => {
            Value::Array(items.into_iter().map(duck_to_json).collect())
        }

        DuckValue::Map(pairs) => {
            // OrderedMap exposes iter() → (&K, &V); clone to consume
            Value::Array(
                pairs
                    .iter()
                    .map(|(k, v)| {
                        serde_json::json!({"key": duck_to_json(k.clone()), "value": duck_to_json(v.clone())})
                    })
                    .collect(),
            )
        }

        DuckValue::Struct(fields) => {
            let mut obj = serde_json::Map::new();
            for pair in fields.iter() {
                obj.insert(pair.0.clone(), duck_to_json(pair.1.clone()));
            }
            Value::Object(obj)
        }

        // Union: unwrap the inner value
        DuckValue::Union(inner) => duck_to_json(*inner),
    }
}

// ── Extension management ──────────────────────────────────────────────────────

#[derive(serde::Serialize)]
pub struct ExtensionInfo {
    pub name: String,
    pub loaded: bool,
    pub installed: bool,
    pub description: String,
}

/// List all DuckDB extensions with their install/load status.
#[tauri::command]
pub async fn list_duckdb_extensions() -> Vec<ExtensionInfo> {
    let task = tokio::task::spawn_blocking(|| {
        let conn = Connection::open_in_memory().ok()?;
        let sql = "SELECT extension_name, loaded, installed, description \
                   FROM duckdb_extensions() \
                   ORDER BY installed DESC, extension_name";
        let mut stmt = conn.prepare(sql).ok()?;
        let rows: Vec<ExtensionInfo> = stmt
            .query_map([], |row| {
                Ok(ExtensionInfo {
                    name: row.get::<_, String>(0).unwrap_or_default(),
                    loaded: row.get::<_, bool>(1).unwrap_or(false),
                    installed: row.get::<_, bool>(2).unwrap_or(false),
                    description: row.get::<_, String>(3).unwrap_or_default(),
                })
            })
            .ok()?
            .flatten()
            .collect();
        Some(rows)
    });
    match task.await {
        Ok(Some(v)) => v,
        _ => vec![],
    }
}

/// Install and load a DuckDB extension by name.
#[tauri::command]
pub async fn install_duckdb_extension(name: String) -> Result<String, String> {
    let task = tokio::task::spawn_blocking(move || {
        let conn = Connection::open_in_memory()
            .map_err(|e| format!("Connection error: {e}"))?;
        conn.execute_batch(&format!("INSTALL '{}';", name))
            .map_err(|e| format!("Install failed: {e}"))?;
        conn.execute_batch(&format!("LOAD '{}';", name))
            .map_err(|e| format!("Load failed: {e}"))?;
        Ok(format!("Extension '{}' installed successfully.", name))
    });
    match task.await {
        Ok(r) => r,
        Err(e) => Err(format!("Task error: {e}")),
    }
}

/// Run a DuckDB query with one or more named connections pre-attached.
///
/// For each connection:
///   1. INSTALL + LOAD the extension if not already loaded.
///   2. ATTACH the connection string (for database-type extensions).
/// Then run the user SQL on the same connection.
#[tauri::command]
pub async fn run_duckdb_query_with_connections(
    sql: String,
    connections: Vec<ConnectionAlias>,
) -> DuckDbResult {
    let task = tokio::task::spawn_blocking(move || {
        let start = std::time::Instant::now();
        let result = execute_sql_with_connections(&sql, &connections);
        let duration_ms = start.elapsed().as_millis() as u64;
        (result, duration_ms)
    });

    match task.await {
        Ok((Ok((data, columns)), duration_ms)) => {
            let shape = [data.len(), columns.len()];
            DuckDbResult { success: true, error: None, data, columns, shape, duration_ms }
        }
        Ok((Err(e), duration_ms)) => DuckDbResult {
            success: false, error: Some(e), data: vec![], columns: vec![], shape: [0, 0], duration_ms,
        },
        Err(e) => DuckDbResult {
            success: false, error: Some(format!("Task error: {e}")), data: vec![], columns: vec![], shape: [0, 0], duration_ms: 0,
        },
    }
}

fn execute_sql_with_connections(
    sql: &str,
    connections: &[ConnectionAlias],
) -> Result<(Vec<serde_json::Map<String, Value>>, Vec<String>), String> {
    let conn = Connection::open_in_memory()
        .map_err(|e| format!("DuckDB connection error: {e}"))?;

    for alias in connections {
        // Install + load the extension
        if let Err(e) = conn.execute_batch(&format!("INSTALL '{}';", alias.ext_type)) {
            // Ignore "already installed" errors
            if !e.to_string().contains("already") {
                return Err(format!("Failed to install extension '{}': {e}", alias.ext_type));
            }
        }
        if let Err(e) = conn.execute_batch(&format!("LOAD '{}';", alias.ext_type)) {
            if !e.to_string().contains("already") {
                return Err(format!("Failed to load extension '{}': {e}", alias.ext_type));
            }
        }

        // Attach database connections
        if !alias.connection_string.is_empty() {
            let attach_type = match alias.ext_type.as_str() {
                "postgres" => Some("POSTGRES"),
                "mysql"    => Some("MYSQL"),
                "sqlite"   => Some("SQLITE"),
                _          => None,
            };
            if let Some(db_type) = attach_type {
                // Sanitise alias name: lowercase, spaces → underscores, alphanumeric + _ only
                let safe_name: String = alias
                    .name
                    .to_lowercase()
                    .chars()
                    .map(|c| if c.is_alphanumeric() { c } else { '_' })
                    .collect();
                let conn_str = alias.connection_string.replace('\'', "''");
                let attach_sql = format!(
                    "ATTACH '{conn_str}' AS {safe_name} (TYPE {db_type}, READ_ONLY);"
                );
                conn.execute_batch(&attach_sql)
                    .map_err(|e| format!("Failed to attach '{}': {e}", alias.name))?;
            }
        }
    }

    execute_sql_on_conn(&conn, sql)
}

// ── Parquet export ────────────────────────────────────────────────────────────

/// Write a result set (already materialized as JSON rows) to a Parquet file.
///
/// Strategy:
///   1. Serialize each row as a newline-delimited JSON record → temp `.ndjson` file.
///   2. Open an in-memory DuckDB connection and run:
///        COPY (SELECT * FROM read_json_auto('<tmp>')) TO '<path>' (FORMAT PARQUET)
///   3. Delete the temp file.
///
/// Returns the number of rows written, or an error string.
#[tauri::command]
pub async fn export_to_parquet(
    data: Vec<serde_json::Map<String, Value>>,
    path: String,
) -> Result<u64, String> {
    let task = tokio::task::spawn_blocking(move || {
        // ── 1. Write NDJSON to a uniquely-named temp file ─────────────────────
        let temp_path = std::env::temp_dir().join(format!(
            "blazer_export_{}.ndjson",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        ));

        {
            let mut file = std::fs::File::create(&temp_path)
                .map_err(|e| format!("Failed to create temp file: {e}"))?;
            for row in &data {
                let line = serde_json::to_string(row)
                    .map_err(|e| format!("JSON serialization error: {e}"))?;
                writeln!(file, "{line}")
                    .map_err(|e| format!("Write error: {e}"))?;
            }
        } // file closed here

        // ── 2. Use DuckDB to write Parquet ────────────────────────────────────
        // Normalise path separators for DuckDB (Windows backslash → forward slash)
        let temp_str = temp_path.to_string_lossy().replace('\\', "/");
        let out_str  = path.replace('\\', "/");

        let sql = format!(
            "COPY (SELECT * FROM read_json_auto('{}')) TO '{}' (FORMAT PARQUET)",
            temp_str.replace('\'', "''"),
            out_str.replace('\'', "''"),
        );

        let conn = Connection::open_in_memory()
            .map_err(|e| format!("DuckDB connection error: {e}"))?;

        let write_result = conn.execute_batch(&sql);

        // ── 3. Clean up temp file regardless of outcome ───────────────────────
        let _ = std::fs::remove_file(&temp_path);

        write_result.map_err(|e| format!("Parquet export failed: {e}"))?;
        Ok(data.len() as u64)
    });

    match task.await {
        Ok(r)  => r,
        Err(e) => Err(format!("Task spawn error: {e}")),
    }
}
