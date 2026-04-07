/**
 * readExpr / resolveReadExpr
 *
 * Build the DuckDB SQL table-expression for a given AttachedFile.
 *
 * For directory types (xlsx_dir, csv_dir) we need the actual file list because
 * DuckDB 1.1.x does NOT allow subqueries inside table function arguments
 * ("Binder Error: Table function cannot contain subqueries") and the glob
 * string pattern in read_xlsx('path/*.xlsx') only unions files in DuckDB ≥ 1.2.
 *
 * resolveReadExpr first runs a glob() query to enumerate every matching file,
 * then builds a literal array  read_xlsx(['f1','f2',...])  that works in all
 * bundled DuckDB versions.
 */

import { invoke } from '@tauri-apps/api/core'
import type { AttachedFile } from './types'

function sqlEscape(p: string) {
  return p.replace(/'/g, "''")
}

/** Sync form — safe for parquet / csv / xlsx (single file). */
export function readExpr(file: AttachedFile): string {
  const ext = file.ext.toLowerCase()
  const p = sqlEscape(file.path)
  if (ext === 'csv' || ext === 'tsv') return `read_csv_auto('${p}')`
  if (ext === 'xlsx') return `read_xlsx('${p}')`
  if (ext === 'parquet_dir' || !ext || ext === '') return `read_parquet('${p}/**/*.parquet')`
  if (ext === 'parquet') return `read_parquet('${p}')`
  // xlsx_dir / csv_dir — caller should use resolveReadExpr instead
  if (ext === 'xlsx_dir') return `read_xlsx('${p}/*.xlsx')`
  if (ext === 'csv_dir') return `read_csv_auto('${p}/*.csv')`
  return `read_parquet('${p}')`
}

/**
 * Async form — for xlsx_dir / csv_dir, runs glob() first to get the real
 * file list and returns  read_xlsx(['f1','f2',...])  which works in DuckDB 1.1.x.
 * Falls back to the sync glob-string form on error.
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
      const list = res.data
        .map(r => `'${String(r['file'] ?? r['path'] ?? '').replace(/'/g, "''")}'`)
        .join(', ')
      return `${fn}([${list}])`
    }
  } catch { /* fall through to glob-string form */ }

  // Fallback: simple glob string (works in DuckDB ≥ 1.2)
  return `${fn}('${p}/${pattern}')`
}
