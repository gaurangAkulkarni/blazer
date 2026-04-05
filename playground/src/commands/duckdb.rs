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

// ── Native execution ──────────────────────────────────────────────────────────

fn execute_sql_native(
    sql: &str,
) -> Result<(Vec<serde_json::Map<String, Value>>, Vec<String>), String> {
    let conn = Connection::open_in_memory()
        .map_err(|e| format!("DuckDB connection error: {e}"))?;

    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("SQL error: {e}"))?;

    // ── Phase 1: execute and collect raw values ───────────────────────────────
    //
    // `Statement::column_names()` panics if called BEFORE the statement has
    // been stepped ("execute"d).  We must not call it here.
    //
    // Instead, collect each row as a plain Vec<DuckValue> using index-based
    // `row.get(i)`.  Inside the query_map closure the statement is already
    // running, so `row.get(i)` returns Err(InvalidColumnIndex) — not a panic —
    // when i ≥ column_count.  We use that as the loop terminator.
    let raw_rows: Vec<Vec<DuckValue>> = stmt
        .query_map([], |row| {
            let mut vals = Vec::new();
            for i in 0_usize.. {
                match row.get::<_, DuckValue>(i) {
                    Ok(v)  => vals.push(v),
                    Err(_) => break,   // i >= column_count → no more columns
                }
            }
            Ok(vals)
        })
        .map_err(|e| format!("Query execution error: {e}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Row collection error: {e}"))?;

    // ── Phase 2: read column names after execution ────────────────────────────
    //
    // Now that query_map has finished stepping through the result set, the
    // statement IS executed and column_names() is safe to call.
    let column_names = stmt.column_names();

    // ── Phase 3: zip names + values → JSON objects ────────────────────────────
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

fn duck_to_json(val: DuckValue) -> Value {
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
