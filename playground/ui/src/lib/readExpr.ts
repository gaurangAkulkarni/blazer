/**
 * readExpr / resolveReadExpr
 *
 * Build the DuckDB SQL table-expression for a given AttachedFile.
 *
 * DuckDB 1.1.x (bundled) limitations for read_xlsx:
 *   - read_xlsx('path/*.xlsx')            → reads only 1 file (no multi-file glob)
 *   - read_xlsx((SELECT list(...) ...))   → Binder Error: subquery in table fn
 *   - read_xlsx(VARCHAR[])               → Binder Error: no VARCHAR[] overload
 *
 * Only reliable approach in 1.1.x:
 *   SELECT * FROM read_xlsx('f1') UNION ALL SELECT * FROM read_xlsx('f2') ...
 *
 * resolveReadExpr() first runs  SELECT file FROM glob(...)  to get every
 * matching path, then returns the UNION ALL expression wrapped in a subquery:
 *   (SELECT * FROM read_xlsx('f1') UNION ALL SELECT * FROM read_xlsx('f2') ...)
 */

import { invoke } from '@tauri-apps/api/core'
import type { AttachedFile } from './types'

function sqlEscape(p: string) {
  return p.replace(/'/g, "''")
}

/** Sync — safe for single-file types (csv, xlsx, parquet). */
export function readExpr(file: AttachedFile): string {
  const ext = file.ext.toLowerCase()
  const p = sqlEscape(file.path)
  if (ext === 'csv' || ext === 'tsv') return `read_csv_auto('${p}')`
  // all_varchar=true: read every cell as text so mixed-type columns (e.g. a column that is
  // mostly numbers but contains 'PUP912') are preserved in full — no data is silently NULLed.
  // Callers can TRY_CAST(col AS DOUBLE) / TRY_CAST(col AS INTEGER) when numeric ops are needed.
  if (ext === 'xlsx') return `read_xlsx('${p}', all_varchar=true)`
  if (ext === 'parquet_dir' || !ext || ext === '') return `read_parquet('${p}/**/*.parquet')`
  if (ext === 'parquet') return `read_parquet('${p}')`
  // JSON / NDJSON — must use read_json_auto, NOT read_parquet
  if (ext === 'json' || ext === 'ndjson' || ext === 'jsonl') return `read_json_auto('${p}')`
  // dirs: caller should use resolveReadExpr()
  if (ext === 'xlsx_dir') return `read_xlsx('${p}/*.xlsx', all_varchar=true)`
  if (ext === 'csv_dir') return `read_csv_auto('${p}/*.csv')`
  if (ext === 'json_dir') return `read_json_auto('${p}/*.ndjson')`
  // Unknown extension — default to parquet (most common analytical format)
  return `read_parquet('${p}')`
}

/**
 * Derive the correct DuckDB reader expression from a bare file path string.
 * Used when we discover actual files inside a folder at runtime (e.g. from
 * describe_tables results) and need to build the reader without an AttachedFile.
 */
export function readerForPath(filePath: string): string {
  const p = filePath.replace(/'/g, "''")
  const ext = filePath.split('.').pop()?.toLowerCase() ?? ''
  if (ext === 'csv' || ext === 'tsv') return `read_csv_auto('${p}')`
  if (ext === 'xlsx') return `read_xlsx('${p}', all_varchar=true)`
  if (ext === 'parquet') return `read_parquet('${p}')`
  if (ext === 'json' || ext === 'ndjson' || ext === 'jsonl') return `read_json_auto('${p}')`
  return `read_parquet('${p}')` // safe default
}

/**
 * Async — for xlsx_dir / csv_dir, runs glob() to list files and returns a
 * UNION ALL expression that reads every file individually.
 * Wraps in a subquery so callers can use it as  SELECT … FROM <expr>.
 */
export async function resolveReadExpr(file: AttachedFile): Promise<string> {
  const ext = file.ext.toLowerCase()

  if (ext !== 'xlsx_dir' && ext !== 'csv_dir') {
    return readExpr(file)
  }

  const p = sqlEscape(file.path)
  const isXlsx = ext === 'xlsx_dir'
  const pattern = isXlsx ? '*.xlsx' : '*.csv'
  const fn = isXlsx ? 'read_xlsx' : 'read_csv_auto'

  try {
    const res = await invoke<{ success: boolean; data: Record<string, unknown>[] }>(
      'run_duckdb_query',
      { sql: `SELECT file FROM glob('${p}/${pattern}') ORDER BY file` },
    )

    if (res?.success && res.data.length > 0) {
      // UNION ALL BY NAME aligns columns by name, filling missing columns with NULL.
      // This handles supplier files that have slightly different column sets.
      const xlsxOpts = isXlsx ? ', all_varchar=true' : ''
      const union = res.data
        .map(r => `SELECT * FROM ${fn}('${String(r['file'] ?? '').replace(/'/g, "''")}' ${xlsxOpts})`)
        .join('\nUNION ALL BY NAME\n')
      // Wrap in a subquery so it can be used anywhere a table expression is expected
      return `(${union})`
    }
  } catch { /* fall through */ }

  // Fallback: glob-string form (works in DuckDB ≥ 1.2, harmless to try)
  return `${fn}('${p}/${pattern}')`
}
