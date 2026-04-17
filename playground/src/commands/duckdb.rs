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

/// Community extensions require `INSTALL name FROM community` instead of just `INSTALL name`.
fn is_community_extension(name: &str) -> bool {
    matches!(name, "mongo_scanner")
}

/// Install + load a single extension, using the correct source (core vs community).
fn install_ext(conn: &Connection, name: &str) -> Result<(), String> {
    let install_sql = if is_community_extension(name) {
        format!("INSTALL '{name}' FROM community;")
    } else {
        format!("INSTALL '{name}';")
    };
    if let Err(e) = conn.execute_batch(&install_sql) {
        if !e.to_string().to_lowercase().contains("already") {
            return Err(format!("Install failed for '{name}': {e}"));
        }
    }
    if let Err(e) = conn.execute_batch(&format!("LOAD '{name}';")) {
        if !e.to_string().to_lowercase().contains("already") {
            return Err(format!("Load failed for '{name}': {e}"));
        }
    }
    Ok(())
}

/// Install and load a DuckDB extension by name.
#[tauri::command]
pub async fn install_duckdb_extension(name: String) -> Result<String, String> {
    let task = tokio::task::spawn_blocking(move || {
        let conn = Connection::open_in_memory()
            .map_err(|e| format!("Connection error: {e}"))?;
        install_ext(&conn, &name)?;
        Ok(format!("Extension '{name}' installed successfully."))
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
        // Install + load the extension (handles core vs community transparently)
        install_ext(&conn, &alias.ext_type)?;

        // ── Database extensions: ATTACH ──────────────────────────────────────
        // MongoDB (mongo_scanner) uses mongo_scan() function-style queries —
        // there is no ATTACH syntax; the URI is passed per-call.
        if !alias.connection_string.is_empty() {
            let attach_type = match alias.ext_type.as_str() {
                "postgres"      => Some("POSTGRES"),
                "mysql"         => Some("MYSQL"),
                "sqlite"        => Some("SQLITE"),
                "mongo_scanner" => None,   // no ATTACH — uses mongo_scan(uri, db, coll)
                _               => None,
            };
            if let Some(db_type) = attach_type {
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

        // ── Path-scan extensions (delta / iceberg): apply Azure credentials ──
        // For ADLS Gen2 (abfss://) paths the azure extension must be loaded and a
        // DuckDB secret created before delta_scan() / iceberg_scan() can work.
        if matches!(alias.ext_type.as_str(), "delta" | "iceberg") {
            let azure_auth = alias.azure_auth.as_deref().unwrap_or("none");
            if azure_auth != "none" && !azure_auth.is_empty() {
                // Load the azure extension (ignore "already loaded" errors)
                if let Err(e) = conn.execute_batch("INSTALL 'azure'; LOAD 'azure';") {
                    if !e.to_string().contains("already") {
                        return Err(format!("Failed to load azure extension: {e}"));
                    }
                }
                match azure_auth {
                    "service_principal" => {
                        if let (Some(t), Some(c), Some(s)) = (
                            &alias.azure_tenant_id,
                            &alias.azure_client_id,
                            &alias.azure_client_secret,
                        ) {
                            if !t.is_empty() && !c.is_empty() && !s.is_empty() {
                                let sql = format!(
                                    "CREATE OR REPLACE SECRET _blazer_azure (\
                                     TYPE AZURE, PROVIDER SERVICE_PRINCIPAL, \
                                     TENANT_ID '{}', CLIENT_ID '{}', CLIENT_SECRET '{}');",
                                    t.replace('\'', "''"),
                                    c.replace('\'', "''"),
                                    s.replace('\'', "''"),
                                );
                                conn.execute_batch(&sql).map_err(|e| {
                                    format!("Azure service-principal secret failed for '{}': {e}", alias.name)
                                })?;
                            }
                        }
                    }
                    "account_key" | "sas" => {
                        if let Some(cs) = &alias.azure_storage_connection_string {
                            if !cs.is_empty() {
                                let sql = format!(
                                    "SET azure_storage_connection_string = '{}';",
                                    cs.replace('\'', "''")
                                );
                                conn.execute_batch(&sql).map_err(|e| {
                                    format!("Azure storage key setup failed for '{}': {e}", alias.name)
                                })?;
                            }
                        }
                    }
                    "azure_cli" => {
                        conn.execute_batch(
                            "CREATE OR REPLACE SECRET _blazer_azure (TYPE AZURE, PROVIDER AZURE_CLI);",
                        )
                        .map_err(|e| format!("Azure CLI auth failed for '{}': {e}", alias.name))?;
                    }
                    _ => {}
                }
            }
        }
    }

    execute_sql_on_conn(&conn, sql)
}

// ── Test connection ───────────────────────────────────────────────────────────

/// Validate that a connection alias can actually reach its target.
/// For database extensions (postgres/mysql/sqlite): ATTACH + DETACH.
/// For path-scan extensions (delta/iceberg): light schema fetch via scan function.
/// Returns a short success message or an error string.
#[tauri::command]
pub async fn test_duckdb_connection(connection: ConnectionAlias) -> Result<String, String> {
    let task = tokio::task::spawn_blocking(move || test_connection_sync(&connection));
    task.await.map_err(|e| format!("Task error: {e}"))?
}

fn test_connection_sync(alias: &ConnectionAlias) -> Result<String, String> {
    let conn = Connection::open_in_memory()
        .map_err(|e| format!("DuckDB init error: {e}"))?;

    // Install + load the extension (handles community extensions transparently)
    let ext = alias.ext_type.as_str();
    install_ext(&conn, ext)?;

    match ext {
        "mongo_scanner" => {
            if alias.connection_string.is_empty() {
                return Err("MongoDB URI is required (e.g. mongodb://host:27017)".to_string());
            }
            // mongo_scanner has no ATTACH — extension loading is the verification.
            // We can't ping a real server without a collection name, so just confirm
            // the extension is loaded and the URI looks valid.
            let uri = alias.connection_string.trim();
            if !uri.starts_with("mongodb://") && !uri.starts_with("mongodb+srv://") {
                return Err("URI must start with mongodb:// or mongodb+srv://".to_string());
            }
            Ok(format!("mongo_scanner loaded — ready to query with mongo_scan('{uri}', 'db', 'collection')"))
        }

        "postgres" | "mysql" | "sqlite" => {
            if alias.connection_string.is_empty() {
                return Err("Connection string is required".to_string());
            }
            let db_type = match ext {
                "postgres" => "POSTGRES",
                "mysql"    => "MYSQL",
                "sqlite"   => "SQLITE",
                _          => unreachable!(),
            };
            let conn_str = alias.connection_string.replace('\'', "''");
            let sql = format!(
                "ATTACH '{conn_str}' AS _blazer_test (TYPE {db_type}, READ_ONLY);"
            );
            conn.execute_batch(&sql)
                .map_err(|e| format!("Connection failed: {e}"))?;
            conn.execute_batch("DETACH _blazer_test;").ok();
            Ok("Connected successfully".to_string())
        }

        "delta" | "iceberg" => {
            if alias.connection_string.is_empty() {
                return Err("Table path is required".to_string());
            }
            // Apply Azure credentials if configured
            let azure_auth = alias.azure_auth.as_deref().unwrap_or("none");
            if azure_auth != "none" && !azure_auth.is_empty() {
                if let Err(e) = conn.execute_batch("INSTALL 'azure'; LOAD 'azure';") {
                    if !e.to_string().contains("already") {
                        return Err(format!("Failed to load azure extension: {e}"));
                    }
                }
                match azure_auth {
                    "service_principal" => {
                        if let (Some(t), Some(c), Some(s)) = (
                            &alias.azure_tenant_id,
                            &alias.azure_client_id,
                            &alias.azure_client_secret,
                        ) {
                            if !t.is_empty() && !c.is_empty() && !s.is_empty() {
                                let sql = format!(
                                    "CREATE OR REPLACE SECRET _blazer_azure (\
                                     TYPE AZURE, PROVIDER SERVICE_PRINCIPAL, \
                                     TENANT_ID '{}', CLIENT_ID '{}', CLIENT_SECRET '{}');",
                                    t.replace('\'', "''"),
                                    c.replace('\'', "''"),
                                    s.replace('\'', "''"),
                                );
                                conn.execute_batch(&sql)
                                    .map_err(|e| format!("Azure auth setup failed: {e}"))?;
                            }
                        }
                    }
                    "account_key" | "sas" => {
                        if let Some(cs) = &alias.azure_storage_connection_string {
                            if !cs.is_empty() {
                                let sql = format!(
                                    "SET azure_storage_connection_string = '{}';",
                                    cs.replace('\'', "''")
                                );
                                conn.execute_batch(&sql)
                                    .map_err(|e| format!("Azure storage setup failed: {e}"))?;
                            }
                        }
                    }
                    "azure_cli" => {
                        conn.execute_batch(
                            "CREATE OR REPLACE SECRET _blazer_azure (TYPE AZURE, PROVIDER AZURE_CLI);",
                        )
                        .map_err(|e| format!("Azure CLI auth failed: {e}"))?;
                    }
                    _ => {}
                }
            }
            // Light schema check — just fetch column names, no data
            let scan_fn = if ext == "delta" { "delta_scan" } else { "iceberg_scan" };
            let path = alias.connection_string.replace('\'', "''");
            conn.execute_batch(&format!("SELECT * FROM {scan_fn}('{path}') LIMIT 0;"))
                .map_err(|e| format!("Cannot access '{}': {e}", alias.connection_string))?;
            Ok("Table accessible".to_string())
        }

        _ => Ok(format!("Extension '{ext}' loaded")),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    fn open_conn() -> Connection {
        Connection::open_in_memory().expect("test connection")
    }

    // execute_sql_on_conn tests
    #[test]
    fn test_simple_select() {
        let conn = open_conn();
        let (data, cols) = execute_sql_on_conn(&conn, "SELECT 42 AS answer").unwrap();
        assert_eq!(cols, vec!["answer"]);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["answer"], serde_json::json!(42));
    }

    #[test]
    fn test_multiple_columns() {
        let conn = open_conn();
        let (data, cols) = execute_sql_on_conn(&conn, "SELECT 1 AS a, 'hello' AS b").unwrap();
        assert_eq!(cols, vec!["a", "b"]);
        assert_eq!(data[0]["b"], serde_json::json!("hello"));
    }

    #[test]
    fn test_multiple_rows() {
        let conn = open_conn();
        let (data, cols) = execute_sql_on_conn(
            &conn,
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)",
        ).unwrap();
        assert_eq!(data.len(), 2);
        assert_eq!(cols, vec!["id", "name"]);
    }

    #[test]
    fn test_invalid_sql_returns_error() {
        let conn = open_conn();
        let result = execute_sql_on_conn(&conn, "SELECT * FROM nonexistent_table_xyz");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("SQL error"));
    }

    #[test]
    fn test_create_and_query_table() {
        let conn = open_conn();
        execute_sql_on_conn(&conn, "CREATE TABLE t (x INTEGER, y TEXT)").unwrap();
        execute_sql_on_conn(&conn, "INSERT INTO t VALUES (1, 'hello'), (2, 'world')").unwrap();
        let (data, cols) = execute_sql_on_conn(&conn, "SELECT x, y FROM t ORDER BY x").unwrap();
        assert_eq!(data.len(), 2);
        assert_eq!(data[0]["x"], serde_json::json!(1));
        assert_eq!(data[0]["y"], serde_json::json!("hello"));
        assert_eq!(data[1]["x"], serde_json::json!(2));
        let _ = cols;
    }

    #[test]
    fn test_null_values() {
        let conn = open_conn();
        let (data, _) = execute_sql_on_conn(&conn, "SELECT NULL AS n").unwrap();
        assert!(data[0]["n"].is_null());
    }

    #[test]
    fn test_float_values() {
        let conn = open_conn();
        let (data, _) = execute_sql_on_conn(&conn, "SELECT 3.14 AS pi").unwrap();
        // DuckDB returns decimal literals as Decimal, which duck_to_json serialises
        // as a String to avoid precision loss.  Accept both Number and String forms.
        let v = &data[0]["pi"];
        let numeric: f64 = if let Some(n) = v.as_f64() {
            n
        } else if let Some(s) = v.as_str() {
            s.parse::<f64>().expect("decimal string should parse to f64")
        } else {
            panic!("unexpected JSON type for pi: {:?}", v);
        };
        assert!((numeric - 3.14).abs() < 0.001);
    }

    #[test]
    fn test_boolean_values() {
        let conn = open_conn();
        let (data, _) = execute_sql_on_conn(&conn, "SELECT true AS t, false AS f").unwrap();
        assert_eq!(data[0]["t"], serde_json::json!(true));
        assert_eq!(data[0]["f"], serde_json::json!(false));
    }

    #[test]
    fn test_empty_result_set() {
        let conn = open_conn();
        execute_sql_on_conn(&conn, "CREATE TABLE empty_t (id INTEGER)").unwrap();
        let (data, cols) = execute_sql_on_conn(&conn, "SELECT id FROM empty_t").unwrap();
        assert_eq!(data.len(), 0);
        assert_eq!(cols, vec!["id"]);
    }

    #[test]
    fn test_aggregation() {
        let conn = open_conn();
        let (data, _) = execute_sql_on_conn(
            &conn,
            "SELECT count(*) AS cnt, sum(x) AS total FROM (VALUES (1), (2), (3)) t(x)",
        ).unwrap();
        assert_eq!(data[0]["cnt"], serde_json::json!(3));
        // sum() returns Decimal in DuckDB, which duck_to_json serialises as a String.
        // Accept both: a JSON Number (int/float) or a String whose value is "6".
        let total = &data[0]["total"];
        let total_val: i64 = if let Some(n) = total.as_i64() {
            n
        } else if let Some(s) = total.as_str() {
            s.parse::<i64>().expect("decimal string should parse to i64")
        } else {
            panic!("unexpected JSON type for total: {:?}", total);
        };
        assert_eq!(total_val, 6);
    }

    #[test]
    fn test_string_with_special_chars() {
        let conn = open_conn();
        let (data, _) = execute_sql_on_conn(&conn, "SELECT 'it''s a test' AS s").unwrap();
        assert_eq!(data[0]["s"], serde_json::json!("it's a test"));
    }

    #[test]
    fn test_syntax_error_message() {
        let conn = open_conn();
        let err = execute_sql_on_conn(&conn, "SELEKT 1").unwrap_err();
        // Error message should mention the issue
        assert!(!err.is_empty());
    }

    // DuckDbResult struct
    #[test]
    fn test_duck_db_result_serializes() {
        let result = DuckDbResult {
            success: true,
            error: None,
            data: vec![],
            columns: vec!["a".to_string()],
            shape: [0, 1],
            duration_ms: 42,
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"duration_ms\":42"));
    }
}
